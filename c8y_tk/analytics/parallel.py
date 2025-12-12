from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, wait
from queue import Queue, Empty
from threading import Event

import math
import pandas as pd

from c8y_api.model import get_by_path
from c8y_api.model._base import as_record

_logger = logging.getLogger(__name__)


def as_list(api, workers: int, strategy: str, **kwargs) -> list:
    q, e = read(api, workers=workers, strategy=strategy, batched=True, **kwargs)
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


def as_records(api, workers: int, strategy: str, mapping: dict, **kwargs) -> list[dict]:
    # always use get_all as batched insertions/readings are generally faster
    q, e = read(api, workers=workers, strategy=strategy, batched=True, **kwargs)

    data = []
    while True:
        try:
            items = q.get_nowait()
        except Empty:
            items = q.get()
        if items is None:
            break
        data.extend(as_record(i, mapping) for i in items)
    return data


def as_dataframe(api, workers: int, strategy: str, columns: list = None, mapping: dict = None, **kwargs) -> pd.DataFrame:
    # always use get_all as batched insertions/readings are generally faster
    q, e = read(api, workers=workers, strategy=strategy, batched=True, **kwargs)

    # --- using tuples/records ---
    # We assume that the select function is invoked with an as_values
    # parameter which already converts the JSON to a tuple/record
    if not mapping:
        # -> results are tuples
        records = []
        while True:
            try:
                items = q.get_nowait()
            except Empty:
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
        try:
            items = q.get_nowait()
        except Empty:
            items = q.get()
        if items is None:
            break
        for name, path in mapping.items():
            data[name].extend(get_by_path(i, path) for i in items)
    return pd.DataFrame.from_dict(data)


def read(api, workers: int, strategy: str, batched: bool, **kwargs) -> (Queue, Event):
    """Run multiple selects in parallel

    inventory_api.select(a=1,b=2,c=3) ->
    queue = select(inventory, workers=50, strategy='pages', a=1, b=2, c=3)
    """
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
        except Exception as ex:
            _logger.error(ex)

    futures = []
    executor = ThreadPoolExecutor(max_workers=workers)
    if strategy.startswith('page'):
        futures = [
            executor.submit(process_page, page_number=p + 1)
            for p in range(expected_pages)
        ]

    end_event = Event()
    def wait_and_close():
        wait(futures)
        queue.put(None)
        end_event.set()

    def shutdown(_):
        executor.shutdown(wait=False)

    executor.submit(wait_and_close).add_done_callback(shutdown)

    return queue, end_event

def collect(workers: int, **kwargs) -> pd.DataFrame:
    pass

