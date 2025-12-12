from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, wait as await_futures
from queue import Queue, Empty

import math
import pandas as pd

from c8y_api.model import as_record, get_by_path, CumulocityResource


_logger = logging.getLogger(__name__)


class ParallelExecutor:
    """Parallel execution context.

    Use this class to run multiple `select`, `get_all` (batched) or `collect`
    (for measurements only) API calls in parallel for better throughput and
    overall performance by reducing I/O wait time.

    The `select`, `get_all`, and `collect` functions can be invoked just as
    if the corresponding API is invoked directly; the additional parameters
    will be passed directly as-is. This includes the `as_values` parameter
    which can be used to directly parse the JSON into tuples.

    This class _should_ be used as a context manager, i.e.
    ```
        with ParallelExecutor() as executor:
            queue = executor.select()
            ...
    ```
    However, it defines multiple static methods which handle the context
    and can be used synchronously, i.e.
    ```
        # read all devices of type 'myType' using threads
        all_devices = ParallelExecutor.as_list(c8y.device_inventory, type='myType'):
    ```

    See also ParallelExecutor.as_list, ParallelExecutor.as_records, ParallelExecutor.as_dataframe
    """

    def __init__(self, workers: int = 5):
        self.workers = workers
        self.executor = None

    def __enter__(self):
        self.executor = ThreadPoolExecutor(max_workers=self.workers)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if self.executor:
                self.executor.shutdown(wait=True)
        finally:
            self.executor = None

    def select(self, api: CumulocityResource, strategy: str = 'pages', **kwargs) -> Queue:
        """Perform multiple `select` API calls in parallel.

        Args:
            api (CumulocityResource): An Cumulocity API instances, e.g. Events
                or Alarms; the API needs to support the `get_count` and
                `select` functions.
            strategy (str): The strategy to use for parallelization;
                Currently, only 'pages' is supported.
            **kwargs: Additional keyword arguments to pass to `select`.

        Returns:
            A Queue instance which is filled asynchronously with the results
            yielded by the `select` function. This may also be a tuple  if the
            `as_values` parameter is utilized to parse the JSON documents.
        """
        return self._read(api, strategy, False, **kwargs)

    def get_all(self, api: CumulocityResource, strategy: str = 'pages', **kwargs) -> Queue:
        """Perform multiple `get_all` API calls in parallel.

        Args:
            api (CumulocityResource): An Cumulocity API instances, e.g. Events
                or Alarms; the API needs to support the `get_count` and
                `get_all` functions.
            strategy (str): The strategy to use for parallelization;
                Currently, only 'pages' is supported.
            **kwargs: Additional keyword arguments to pass to `get_all`.

        Returns:
            A Queue instance which is filled asynchronously with the list
            results returned by the `get_all` function. This may also be a
            list of tuples if the `as_values` parameter is utilized to parse
            the JSON documents.
        """
        return self._read(api, strategy, True, **kwargs)

    def _read(self, api: CumulocityResource, strategy: str, batched: bool, **kwargs) -> Queue:
        # api needs to support `get_count` and `select` functions
        read_fn = 'get_all' if batched else 'select'
        for fun in ('get_count', read_fn):
            if not hasattr(api, fun):
                raise AttributeError(f"Provided API does not support '{fun}' function.")

        # determine expected number of pages
        default_page_size = 1000
        page_size = kwargs.get('page_size', default_page_size)
        expected_total = api.get_count(**kwargs)
        expected_pages = math.ceil(expected_total / page_size)

        # prepare arguments
        kwargs['page_size'] = page_size

        # define worker function
        queue = Queue(maxsize=expected_total)
        read_fun = getattr(api, read_fn)

        def process_page(page_number: int):
            try:
                if batched:
                    queue.put(read_fun(page_number=page_number, **kwargs))
                else:
                    for x in read_fun(page_number=page_number, **kwargs):
                        queue.put(x)
            except Exception as ex:  # pylint: disable=broad-exception-caught
                _logger.error(ex)

        futures = []
        if strategy.startswith('page'):
            futures = [
                self.executor.submit(process_page, page_number=p + 1)
                for p in range(expected_pages)
            ]

        def wait_and_close():
            await_futures(futures)
            queue.put(None)  # sentinel

        self.executor.submit(wait_and_close)

        return queue

    @staticmethod
    def as_list(api, workers: int = 5, strategy: str = 'pages', **kwargs) -> list:
        """Read data via a Cumulocity API concurrently.

        Args:
            api (CumulocityResource): An Cumulocity API instances; e.g.
                Events or Alarms. The API needs to support the `get_count`
                and `get_all` functions.
            workers (int): The number of parallel processes to use
            strategy (str): The strategy to use for parallelization;
                Currently, only 'pages' is supported.
            **kwargs: Additional keyword arguments to pass to the underlying
                API calls.

        Returns:
            The collected data as list; These can Python objects or tuples
            if the `as_values` parameter is utilized to parse the objects.
        """
        with ParallelExecutor(workers=workers) as executor:
            q = executor.get_all(api, strategy=strategy, **kwargs)
            result = []
            while True:
                try:
                    items = q.get_nowait()
                except Empty:
                    items = q.get()
                if items is None:
                    break
                result.extend(items)
            return result


    @staticmethod
    def as_records(api, workers: int = 5, strategy: str = 'pages', mapping: dict = None, **kwargs) -> list[dict]:
        """Read data via a Cumulocity API concurrently.

        Args:
            api (CumulocityResource): An Cumulocity API instances; e.g.
                Events or Alarms. The API needs to support the `get_count`
                and `get_all` functions.
            workers (int): The number of parallel processes to use
            strategy (str): The strategy to use for parallelization;
                Currently, only 'pages' is supported.
            mapping (dict): A mapping of simplified JSON paths to record
                field names.
            **kwargs: Additional keyword arguments to pass to the underlying
                API calls.

        Returns:
            The collected data as list of records/dictionaries.

        See also `c8y_api.model.as_records` for more information about the
        mapping syntax.
        """
        with ParallelExecutor(workers=workers) as executor:
            q = executor.get_all(api, strategy=strategy, **kwargs)
            data = []
            while True:
                items = q.get()
                if items is None:
                    break
                data.extend(as_record(i, mapping) for i in items)
            return data


    @staticmethod
    def as_dataframe(
            api,
            workers: int = 5,
            strategy: str = 'pages',
            columns: list = None,
            mapping: dict = None,
            **kwargs) -> pd.DataFrame:
        """Read data via a Cumulocity API concurrently.

        If `mapping` is provided, the API call results are mapped as
        corresponding columns within the result dataframe. Otherwise, it is
        assumed that the `as_values` parameter is provided (for the
        underlying API calls) and the result tuples are directly mapped to
        columns within the result dataframe. The `columns` parameter can
        be used to provide specific column names (default: c1, c2 ...).

        Args:
            api (CumulocityResource): An Cumulocity API instances; e.g.
                Events or Alarms. The API needs to support the `get_count`
                and `get_all` functions.
            workers (int): The number of parallel processes to use
            strategy (str): The strategy to use for parallelization;
                Currently, only 'pages' is supported.
            mapping (dict): A mapping of simplified JSON paths to columns.
            columns (list): A list of column names.
            **kwargs: Additional keyword arguments to pass to the underlying
                API calls.

        Returns:
            The collected data as Pandas DataFrame.

        See also `c8y_api.model.as_` for more information about the
        mapping syntax.
        """
        with ParallelExecutor(workers=workers) as executor:
            q = executor.get_all(api, strategy=strategy, **kwargs)

            # --- using tuples/records ---
            # We assume that the select function is invoked with an as_values
            # parameter which already converts the JSON to a tuple/record
            if not mapping:
                # -> results are tuples
                records = []
                while True:
                    items = q.get()
                    if items is None:
                        break
                    records.extend(items)
                columns = columns or [f'c{i}' for i in range(len(records[0]))]
                return pd.DataFrame.from_records(records, columns=columns)

            # --- using mapping ---
            # We assume that the select function returns plain JSON and the
            # mapping dictionary is used to extract the individual column values
            data = {k: [] for k in mapping.keys()}
            while True:
                items = q.get()
                if items is None:
                    break
                for name, path in mapping.items():
                    data[name].extend(get_by_path(i, path) for i in items)
            return pd.DataFrame.from_dict(data)
