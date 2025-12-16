import threading
import time
from functools import partial
from queue import Empty, Queue
from unittest.mock import Mock, patch

import numpy as np
import pytest
from faker import Faker
from math import ceil
from pytest_benchmark.plugin import benchmark

from c8y_tk.analytics import parallel
from c8y_tk.analytics.parallel import ParallelExecutor


@pytest.mark.parametrize('total, batch_size', [(100000, 0), (100000, 10), (100000, 100), (100000, 1000), ])
@pytest.mark.benchmark
def test_queue_loading(benchmark, total, batch_size):
    """Benchmark queue loading."""

    fake = Faker()
    q = Queue()

    # create data
    data = [fake.name() for i in range(total)]

    def run():
        if batch_size:
            for b in range(total // batch_size):
                q.put([data[i] for i in range(b * batch_size, (b + 1) * batch_size)])
        else:
            for i in range(total):
                q.put(data[i])

    benchmark(run)


@pytest.mark.parametrize('total, batch_size', [(100000, 0), (100000, 10), (100000, 100), (100000, 1000), ])
@pytest.mark.benchmark
def test_queue_unloading(benchmark, total, batch_size):
    """Benchmark queue unloading."""

    # create data
    fake = Faker()
    data = [fake.name() for i in range(total)]

    q = Queue()
    if batch_size:
        for b in range(total // batch_size):
            q.put([data[i] for i in range(b * batch_size, (b + 1) * batch_size)])
    else:
        for i in range(total):
            q.put(data[i])

    # reading from queue into a single huge list
    result = []

    def run():
        while True:
            try:
                if batch_size:
                    result.extend(q.get_nowait())
                else:
                    result.append(q.get_nowait())
            except Empty:
                break

    benchmark(run)

# === queue concurrent loading/unloading performance benchmarking

# This benchmark tests different queue unloading strategies in various
# scenarios (batched, not batched, with delay, without delay between
# batches).

def consume1(q):
    """Consumer variant #1 - simply using q.get() (blocking)."""
    result = []
    while True:
        x = q.get()
        if x is None:
            break
        if isinstance(x, list):
            result.extend(x)
        else:
            result.append(x)
    return result


def consume2(q):
    """Consumer variant #2 - using q.get_nowait and manually release GIL."""
    result = []
    while True:
        try:
            x = q.get_nowait()
            if x is None:
                break
            if isinstance(x, list):
                result.extend(x)
            else:
                result.append(x)
        except Empty:
            time.sleep(0.001)
    return result


def consume3(q):
    """Consumer variant #3 - using q.get_nowait as much as possible and block
    using q.get() if need be,"""
    result = []
    while True:
        try:
            x = q.get_nowait()
        except Empty:
            x = q.get()
        if x is None:
            break
        if isinstance(x, list):
            result.extend(x)
        else:
            result.append(x)
    return result


@pytest.mark.parametrize('consumer_fun', [consume1, consume2, consume3])
@pytest.mark.parametrize('batched', [True, False], ids=['batched', 'single'])
@pytest.mark.parametrize('num_batches, batch_size, delay', [
    (1000, 10, 0),
    (100, 100, 0),
    (1000, 100, 0.005),
    (100, 100, 0.005),
    (1000, 10, 0.01),
    (100, 100, 0.01),
])
def test_queue_performance(benchmark, num_batches, batch_size, delay, batched, consumer_fun):

    q = Queue()

    def produce():
        for b in range(num_batches):
            if batched:
                q.put([b * batch_size + i for i in range(batch_size)])
            else:
                for i in range(batch_size):
                    q.put(b * batch_size + i)
            time.sleep(delay)
        q.put(None)


    def run():
        producer_thread = threading.Thread(target=produce)
        producer_thread.start()

        result = consumer_fun(q)
        producer_thread.join()
        assert len(result) == batch_size * num_batches

    benchmark.group = delay
    benchmark(run)


@pytest.mark.parametrize('workers, total, page_size, result_total', [
    (1, 50, 10, 50),  # singleton worker
    (5, 50, 10, 50),  # each worker will be used
    (10, 50, 10, 50),  # unused workers
    (3, 51, 10, 60),  # one additional page is read
])
def test_not_batched(workers, total, page_size, result_total):
    """Verify that parallel reading works in unbatched mode.

    This test mocks the select method of an API mock as well as the
    corresponding get_count method. The mocked select function returns
    exactly one full page, hence the number of results is a multiple of that.

    The number of workers should not affect the result at all.

    This test drains the queue and verifies that the select function was
    invoked properly which all parameters correct page size and page number.
    """

    def mock_select(page_number, **_kwargs):
        for i in range(page_size):
            yield (page_number - 1) * page_size + i

    api = Mock()
    api.get_count = Mock(return_value=total)
    api.select = Mock(side_effect=mock_select)

    with ParallelExecutor(workers=workers) as executor:
        q = executor.select(api, page_size=page_size, p1=1, p2=False, p3='p3')

        # manually drain queue
        time.sleep(0.1)
        result = []
        while True:
            item = q.get()
            if item is None:
                break
            result.append(item)

    # -> results should all be collected
    assert len(result) == result_total
    assert set(result) == set(range(result_total))
    # -> get_all should have been invoked total/page_size times
    assert api.select.call_count == ceil(total / page_size)  # round up if not perfect
    # -> page_number, page_size and kwargs should be propagated
    for i, c in enumerate(api.select.call_args_list):
        assert c.kwargs['page_size'] == page_size
        assert c.kwargs['p1'] == 1
        assert c.kwargs['p2'] == False
        assert c.kwargs['p3'] == 'p3'
    # -> page numbers might come out of order
    assert {c.kwargs['page_number'] for c in api.select.call_args_list} == set(range(1, ceil(total / page_size) + 1))


@pytest.mark.parametrize('workers, total, page_size, result_total', [
    (1, 50, 10, 50),  # singleton worker
    (5, 50, 10, 50),  # each worker will be used
    (10, 50, 10, 50),  # unused workers
    (3, 51, 10, 60),  # one additional page is read
])
def test_as_list(workers, total, page_size, result_total):
    """Verify that parallel reading works for list results (batched mode).

    This test mocks the get_all method of an API mock as well as the
    corresponding get_count method. The mocked get_all function returns
    exactly one full page, hence the number of results is a multiple of that.

    The number of workers should not affect the result at all.
    """

    def mock_get_all(page_number, **_kwargs):
        return [(page_number - 1) * page_size + x for x in range(page_size)]

    api = Mock()
    api.get_count = Mock(return_value=total)
    api.get_all = Mock(side_effect=mock_get_all)

    result = ParallelExecutor.as_list(api, workers=workers, strategy='pages', page_size=page_size, p1=1, p2=False, p3='p3')

    # -> results should all be collected
    assert len(result) == result_total
    assert set(result) == set(range(result_total))
    # -> get_all should have been invoked total/page_size times
    assert api.get_all.call_count == ceil(total / page_size)
    # -> page_number, page_size and kwargs should be propagated
    for i, c in enumerate(api.get_all.call_args_list):
        assert c.kwargs['page_size'] == page_size
        assert c.kwargs['p1'] == 1
        assert c.kwargs['p2'] == False
        assert c.kwargs['p3'] == 'p3'
    # -> page numbers might come out of order
    assert {c.kwargs['page_number'] for c in api.get_all.call_args_list} == set(range(1, ceil(total / page_size) + 1))


def test_as_records():
    """Verify that the as_records method works as expected."""
    batches = 5
    page_size = 10
    mapping = {'num': 'values.num', 'batch': 'values.batch', 'float': 'values.float', 'bool': 'values.bool'}

    q = Queue()

    # create fake result to work with - tuple for columns dict for mapping
    for b in range(batches):
        q.put([{'values': {'num': b * page_size + p, 'batch': b, 'float': b / 2, 'bool': b % 2 == 0}} for p in
            range(page_size)])
    q.put(None)  # sentinel

    # convert to dataframe
    with patch("c8y_tk.analytics.parallel.ParallelExecutor._read", return_value=q):
        records = ParallelExecutor.as_records(api=None, mapping=mapping)

    assert len(records) == batches * page_size
    for i, r in enumerate(records):
        assert r['num'] == i
        assert r['batch'] == i // page_size
        assert r['float'] == r['batch'] / 2
        assert r['bool'] == (r['batch'] % 2 == 0)


@pytest.mark.parametrize('use_mapping', [True, False], ids=['mapping', 'columns'])
def test_as_dataframe(use_mapping):
    """Verify that the as_dataframe method works as expected."""

    batches = 5
    page_size = 10
    columns = ['num', 'batch', 'float', 'bool']
    mapping = {'num': 'values.num', 'batch': 'values.batch', 'float': 'values.float', 'bool': 'values.bool'}

    q = Queue()

    # create fake result to work with - tuple for columns dict for mapping
    for b in range(batches):
        if use_mapping:
            q.put([{'values': {'num': b * page_size + p, 'batch': b, 'float': b / 2, 'bool': b % 2 == 0}} for p in
                range(page_size)])
        else:
            q.put([(b * page_size + p, b, b / 2, b % 2 == 0) for p in range(page_size)])
    q.put(None)  # sentinel

    # convert to dataframe
    with patch("c8y_tk.analytics.parallel.ParallelExecutor._read", return_value=q):
        df = ParallelExecutor.as_dataframe(
            api=None,
            mapping=mapping if use_mapping else None,
            columns=columns if not use_mapping else None
        )

    # -> 2 columns, batches * page size rows
    assert df.shape == (batches * page_size, len(columns))
    # -> column names are used
    assert df.columns.tolist() == columns
    # -> values are as expected
    assert df.num.to_list() == list(range(batches * page_size))
    assert df.batch.to_list() == list(n // page_size for n in range(batches * page_size))
    # -> types match
    assert df.num.dtype == np.dtype('int')
    assert df.batch.dtype == np.dtype('int')
    assert df['float'].dtype == np.dtype('float')
    assert df['bool'].dtype == np.dtype('bool')


def test_parallel():
    """Verify that parallel execution works as expected."""

    def fun(arg):
        time.sleep(0.1)
        return arg

    with ParallelExecutor(10) as executor:
        # single function
        result = executor.parallel(partial(fun, 12)).as_list()
        assert result == [12]
        # multiple functions
        result = executor.parallel(partial(fun, 12), partial(fun, 13)).as_list()
        assert result == [12, 13]
        # generator
        result = executor.parallel(partial(fun, i) for i in range(100)).as_list()
        assert result == list(range(100))
        # list
        result = executor.parallel([partial(fun, i) for i in range(100)]).as_list()
        assert result == list(range(100))


def test_parallel_as_dataframe():
    """Verify that the as_dataframe method works as expected."""

    def fun1(arg):
        time.sleep(0.1)
        return arg, arg*2

    def fun2(arg):
        time.sleep(0.1)
        return {"i": arg, "j": arg*2}

    ns = list(range(100))
    expected_i = list(range(100))
    expected_j = list(range(0, 200, 2))

    with ParallelExecutor(10) as executor:
        df = executor.parallel(
            partial(fun1, i) for i in ns
        ).as_dataframe()
        assert df.shape == (100, 2)
        assert df.c0.to_list() == expected_i
        assert df.c1.to_list() == expected_j

        df = executor.parallel(
            partial(fun1, i) for i in ns
        ).as_dataframe(columns=['i', 'j'])
        assert df.shape == (100, 2)
        assert df.i.to_list() == expected_i
        assert df.j.to_list() == expected_j

        df = executor.parallel(
            partial(fun2, i) for i in ns
        ).as_dataframe(mapping={'ii': 'i', 'jj': 'j'})
        assert df.shape == (100, 2)
        assert df.ii.to_list() == expected_i
        assert df.jj.to_list() == expected_j
