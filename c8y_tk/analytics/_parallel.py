# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, wait as await_futures, as_completed
from queue import Queue, Empty
from typing import Iterable

import math
import pandas as pd
from pandas import DataFrame

from c8y_api.model import as_record, get_by_path, CumulocityResource
from c8y_api.model._util import _DateUtil

_logger = logging.getLogger(__name__)


class ParallelExecutorResult:
    """(Future) result of a parallel function execution.

    See also: ParallelExecutor.parallel
    """

    def __init__(self, futures):
        self.futures = futures
        self.done = False

    def wait(self):
        """Explicitly wait for the futures to complete.

        Note: this function is implicitly on demand.
        """
        if not self.done:
            await_futures(self.futures)
            self.done = True

    def as_list(self, batch_mode: bool = False) -> list:
        """Collect the results as a list.

        Args:
            batch_mode (bool): If False (default), all futures are awaited
                and their result is returned as a list in order.
                If True, it is assumed that the future results are iterables
                of their own; they are processed as they complete and
                flattened into a single list.
        """
        if batch_mode:
            result = []
            for f in as_completed(self.futures):
                result.extend(f.result())
            return result
        self.wait()
        return [f.result() for f in self.futures]

    def as_dataframe(self, mapping: dict = None, columns: list = None, batch_mode: bool = False) -> DataFrame:
        """Collect the results as a Pandas dataframe.

        If `mapping` is provided, the function call results (dict-like or
        JSON as returned by the API) are mapped as corresponding columns
        within the result dataframe. Otherwise, it is assumed that the
        functions return identically shaped tuples, e.g. by using the
        `as_values` parameter in API calls. The result tuples are directly
        mapped to columns within the result dataframe. The `columns`
        parameter can be used to provide specific column names (default:
        c0, c1 ...).

        Args:
            mapping (dict): A mapping of simplified JSON paths to columns.
            columns (list): A list of column names.
            batch_mode (bool): If False (default), all futures are awaited
                and their result is returned as a list in order.
                If True, it is assumed that the future results are iterables
                of their own; they are processed as they complete and
                flattened into a single list.

        Returns:
            The collected data as Pandas DataFrame.

        See also `c8y_api.model.as_` for more information about the
        mapping syntax.
        """
        results = self.as_list(batch_mode=batch_mode)

        # --- using tuples/records ---
        # We assume that the select function is invoked with an as_values
        # parameter which already converts the JSON to a tuple/record
        if not mapping:
            # -> results are tuples
            columns = columns or [f'c{i}' for i in range(len(results[0]))]
            return pd.DataFrame.from_records(results, columns=columns)

        # --- using mapping ---
        # We assume that the select function returns plain JSON and the
        # mapping dictionary is used to extract the individual column values
        data = {name: [get_by_path(x, path) for x in results]for name, path in mapping.items()}
        return pd.DataFrame.from_dict(data)

    def as_records(self, mapping: dict, batch_mode: bool = False) -> list[dict]:
        """Collect the results as a list of records.

        Args:
            mapping (dict): A mapping of simplified JSON paths to record
                field names.
            batch_mode (bool): If False (default), all futures are awaited
                and their result is returned as a list in order.
                If True, it is assumed that the future results are iterables
                of their own; they are processed as they complete and
                flattened into a single list.

        Returns:
            The collected data as list of records/dictionaries.

        See also `c8y_api.model.as_records` for more information about the
        mapping syntax.
        """
        results = self.as_list(batch_mode=batch_mode)
        return [as_record(x, mapping) for x in results]


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

    def parallel(self, *functions) -> ParallelExecutorResult:
        """Perform a collection of functions in parallel.

        Args:
            functions: Iterable of functions to run in parallel; this can
            be variable args or any iterable including a generator.

        Returns:
            A ParallelExecutorResult object which provides functionality
            conveniently collect the results.

        Note: Do not use `lambda` to define function with bound variables;
        use `functools.partial` instead.
        See also: https://stackoverflow.com/questions/23400785
        """
        if len(functions) == 1:
            if isinstance(functions[0], Iterable):
                functions = functions[0]

        return ParallelExecutorResult([self.executor.submit(f) for f in functions])

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
                Currently, strategy 'pages' and 'dates' are supported. The
                'dates' strategy required a defined date range; the upper
                 bound is assumed to be 'now' of omitted.
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
        default_page_size = 100
        page_size = kwargs.get('page_size', default_page_size)
        expected_total = api.get_count(**kwargs)
        expected_pages = math.ceil(expected_total / page_size)

        # prepare arguments
        kwargs['page_size'] = page_size

        # define worker function
        queue = Queue(maxsize=expected_pages if batched else expected_total)
        read_fun = getattr(api, read_fn)

        def process_page(**_kwargs):
            try:
                if batched:
                    queue.put(read_fun(**_kwargs, **kwargs))
                else:
                    for x in read_fun(**_kwargs, **kwargs):
                        queue.put(x)
            except Exception as ex:  # pylint: disable=broad-exception-caught
                _logger.error(ex)

        futures = []
        # --- pages strategy ---------
        if strategy.startswith('page'):
            futures = [
                self.executor.submit(process_page, page_number=p+1)
                for p in range(expected_pages)
            ]

        # --- date strategy ---------
        elif strategy.startswith('date'):
            # read predefined date range
            date_from = kwargs.get('date_from', kwargs.get('after', None))
            if not date_from:
                raise AttributeError("At least the start of a date range is required for the 'dates' strategy")
            date_from = _DateUtil.ensure_datetime(date_from)
            date_to = _DateUtil.ensure_datetime(kwargs.get('date_to', kwargs.get('before', _DateUtil.now())))
            # remove date parameters as they will be defined dynamically
            kwargs = {k: v for k, v in kwargs.items() if k not in ('after', 'before', 'date_from', 'date_to')}
            # calculate boundary dates for our pages
            delta = (date_to - date_from) / expected_pages
            dates = [date_from + x * delta for x in range(expected_pages+1)]
            # submit page requests
            futures = [
                self.executor.submit(process_page, date_from=dates[p], date_to=dates[p+1])
                for p in range(expected_pages)
            ]

        # ensure that a sentinel element is added to the end
        def wait_and_close():
            await_futures(futures)
            queue.put(None)

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
        be used to provide specific column names (default: c0, c1 ...).

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
