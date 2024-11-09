# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.
import itertools
from datetime import datetime
import json
import os

import pytest

from c8y_api.model import Measurement, Series


def test_measurement_parsing():
    """Verify that parsing of a Measurement works as expected."""
    measurement_json = {
        'id': '12345',
        'self': 'https://...',
        'type': 'c8y_Measurement',
        'source': {'id': '54321', 'self': 'https://...'},
        'time': '2020-31-12T22:33:44,567Z',
        'c8y_Measurement': {'c8y_temperature': {'unit': 'x', 'value': 12.3}}
    }
    m = Measurement.from_json(measurement_json)

    assert m.id == '12345'
    assert m.source == '54321'
    assert m.type == 'c8y_Measurement'
    assert m.time == '2020-31-12T22:33:44,567Z'
    assert m.c8y_Measurement.c8y_temperature.value == 12.3

    expected_full_json = {
        'type': m.type,
        'source': {'id': m.source},
        'time': m.time,
        'c8y_Measurement': {'c8y_temperature': {'unit': 'x', 'value': 12.3}}
    }
    assert m.to_full_json() == expected_full_json


def generate_series_data() -> tuple:
    """Generate all kinds of combinations of series fragments."""

    def gen_level2(n):
        level2_single = ({n: {'1': {'value': 1}}}, [f'{n}.1'])
        level2_multi = ({n: {'1': {'value': 1}, '2': {'value': 2}}}, [f'{n}.1', f'{n}.2'])
        level2_invalid = ({n: {'1': {'data': 1}}}, [])
        level2_mix1 = ({n: {'1': {'data': 1}, '2': {'value': 2}}}, [f'{n}.2'])
        level2_mix2 = ({n: {'1': {'value': 1}, '2': {'data': 2}}}, [f'{n}.1'])

        return [level2_single, level2_multi, level2_invalid, level2_mix1, level2_mix2]

    a = gen_level2('A')
    b = gen_level2('B')
    # collecting combinations of A and B cases
    # (first element is the json, 2nd the expectation for each combination)
    ab = [({**r[0][0], **r[1][0]}, r[0][1] + r[1][1]) for r in itertools.product(a, b)]

    cases = a + ab
    # id is the beautified expectation, prefixed with a number
    ids = [f'{i}: ' + ','.join(map(lambda x: x.replace('.', ''), x[1])) for i, x in enumerate(cases)]

    return cases, ids


@pytest.mark.parametrize('testcase', generate_series_data()[0], ids=generate_series_data()[1])
def test_get_series(testcase):
    """Verify that the get_series function works as expected."""
    data = {**testcase[0], 'source': {'id': '1'}}
    m = Measurement.from_json(data)
    # expected = [*xs for xs in testcase[1]]
    assert testcase[1] == m.get_series()


@pytest.fixture(name='sample_series')
def fix_sample_series():
    """Verify that parsing an Operation from JSON works and provide this
    as a fixture for other tests."""
    path = os.path.dirname(__file__) + '/series.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        series_json = json.load(f)

    return Series(series_json)


def test_collect_single_series_single_value(sample_series: Series):
    """Test collecting a single value (min or max) from a single series."""
    for s in sample_series.specs:
        values = sample_series.collect(series=s.series, value='min')
        # -> None values should be filtered out
        assert all(values)
        assert len(values) < len(sample_series['values'])
        # -> all values should be of the same type
        t = type(values[0])
        assert all(isinstance(v, t) for v in values)


def test_collect_single_series_single_value_with_timestamp(sample_series: Series):
    """Test collecting a single value (min or max) with timestamps from a
    single series."""
    for s in sample_series.specs:
        values = sample_series.collect(series=s.series, value='min', timestamps=True)
        # -> None values should be filtered out
        assert all(values)
        assert len(values) < len(sample_series['values'])
        # -> all values should be 2-tuples (timestamp, value)
        assert all(isinstance(v, tuple) for v in values)
        # -> all timestamps should be strings
        assert all(isinstance(v[0], str) for v in values)
        # -> all values (2nd element) should have same type
        t = type(values[0][1])
        assert all(isinstance(v[1], t) for v in values)


def test_collect_single_series(sample_series: Series):
    """Test collecting all values (min and max) from a single series."""
    for s in sample_series.specs:
        values = sample_series.collect(series=s.series)
        # -> None values should be filtered out
        assert all(values)
        assert len(values) < len(sample_series['values'])
        # -> all values should be 2-tuples (min, max)
        assert all(isinstance(v, tuple) for v in values)
        # -> all min/max values should have same type
        t = type(values[0][0])
        assert all(isinstance(v[1], t) for v in values)


def test_collect_single_series_with_timestamp(sample_series: Series):
    """Test collecting all values (min and max) plus timestamp from a
    single series.

    The result should be a list of 3-tuples, each of which contains the
    timestamp plus min and max value of that series at that timestamp:

        [ (<timestamp>, 4, 5), (<timestamp>, 7, 8), ... ]

    There are no None values, they are filtered out when looking at just
    one series.
    """
    for s in sample_series.specs:
        values = sample_series.collect(series=s.series, timestamps='datetime')
        # -> None values should be filtered out
        assert all(values)
        assert len(values) < len(sample_series['values'])
        # -> all values should be 3-tuples (timestamp, min, max)
        assert all(isinstance(v, tuple) for v in values)
        assert all(len(v) == 3 for v in values)
        # -> all timestamps (1st element) should be datetime
        assert all(isinstance(v[0], datetime) for v in values)
        # -> all min/max values (2nd/3rd element) should have same type
        t = type(values[0][1])
        assert all(isinstance(v[2], t) for v in values)


def test_collect_multiple_series_single_value(sample_series: Series):
    """Test collecting a single value (min or max) from multiple series.

    The result should be a list of tuples, each of which contains the actual
    value at a single time for each series:

        [ (4, 0.4), (5, 0.99), ..., (None, 0.21), ..., (12, None), ... ]

    As we are collecting values from multiple series, there might be None
    values (whenever a series has a value at a specific timestamp or not).
    """
    series_names = [spec.series for spec in sample_series.specs]
    values = sample_series.collect(series=series_names, value='min')
    # -> each value should be an n-tuple (one for each series)
    assert all(isinstance(v, tuple) for v in values)
    assert all(len(v) == len(series_names) for v in values)
    # -> no values should have been filtered
    assert len(values) == len(sample_series['values'])

    # -> each element in the tuple should have the same type
    #    (unless they are None)
    for i in range(0, len(series_names)):
        t = type(values[0][i])
        assert t is not tuple
        assert all(isinstance(v[i], t) for v in values if v[i])


def test_collect_multiple_series_single_value_with_timestamp(sample_series: Series):
    """Test collecting a single value (min or max) from multiple series.
    (including timestamp).

    The result should be a list of tuples, each of which contains the timestamp
    and actual value at a single time for each series:

        [ (<timestamp>, 4, 0.4), (<timestamp>, 5, 0.99), ...,
          (timestamp, None, 0.21), ..., (timestamp, 12, None), ... ]

    As we are collecting values from multiple series, there might be None
    values (whenever a series has a value at a specific timestamp or not).
    """
    series_names = [spec.series for spec in sample_series.specs]
    values = sample_series.collect(series=series_names, value='min', timestamps=True)
    # -> each value should be an n-tuple (one for each series + timestamp)
    assert all(isinstance(v, tuple) for v in values)
    assert all(len(v) == len(series_names) + 1  for v in values)
    # -> no values should have been filtered
    assert len(values) == len(sample_series['values'])

    # -> each element in the n-tuple should be an m-tuple
    #    timestamp + values for each series
    assert all(isinstance(v[0], str) for v in values)
    # -> subsequent elements should all have the same type
    #    (if they are not None)
    for i in range(1, len(series_names)+1):
        t = type(values[0][i])
        assert all(isinstance(v[i], t) for v in values if v[i])


def test_collect_multiple_series(sample_series: Series):
    """Test collecting all values (min and max) from multiple series.

    The result should be a list of n-tuples (n = number or series), each
    of which contains a 2-tuple (min,max):

        [ ((4,5), (0.4, 0.5)), ((5,5), (0.99, 1.02)), ...,
          (None, (0.21, 0.25)), ..., ((12,15), None), ... ]

    As we are collecting values from multiple series, there might be None
    values (whenever a series has a value at a specific timestamp or not).
    """
    series_names = [spec.series for spec in sample_series.specs]
    values = sample_series.collect(series=series_names)
    # -> each value should be an n-tuple (one for each series)
    assert all(isinstance(v, tuple) for v in values)
    assert all(len(v) == len(series_names) for v in values)
    # -> no values should have been filtered
    assert len(values) == len(sample_series['values'])

    # -> each element in the n-tuple should be a 2-tuple
    #    (min, max - unless they are None)
    for i in range(0, len(series_names)):
        assert all(isinstance(v[i], tuple) for v in values if v[i])
        assert all(len(v[i]) == 2 for v in values if v[i])


def test_collect_multiple_series_with_timestamp(sample_series: Series):
    """Test collecting all values (min and max) from multiple series.

    The result should be a list of n-tuples (n = number or series), each
    of which contains a 2-tuple (min,max) for each series

        [ (<timestamp>, (4,5), (0.4, 0.5)), (<timestamp>, (5,5), (0.99, 1.02)), ...,
          (<timestamp>, None, (0.21, 0.25)), ..., (<timestamp>, (12,15), None), ... ]

    As we are collecting values from multiple series, there might be None
    values (whenever a series has a value at a specific timestamp or not).
    """
    series_names = [spec.series for spec in sample_series.specs]
    values = sample_series.collect(series=series_names, timestamps='datetime')

    # -> each value should be an n-tuple (one for each series plus timestamp)
    assert all(isinstance(v, tuple) for v in values)
    assert all(len(v) == len(series_names) + 1 for v in values)
    # -> no values should have been filtered
    assert len(values) == len(sample_series['values'])

    # -> the first element in each n-tuple should be the timestamp
    assert all(isinstance(v[0], datetime) for v in values)

    # -> subsequent elements should all be 2-tuples, one for each series
    #    (unless they are None, indicating that a series did not define a
    #    value at this timestamp)
    for i in range(1, len(series_names)+1):
        assert all(isinstance(v[i], tuple) for v in values if v[i])
        assert all(len(v[i]) == 2 for v in values if v[i])

    # -> if not None, each element in the 2-tuple (min, max) have same type
    # pylint: disable=unidiomatic-typecheck
    for i in range(1, len(series_names)+1):
        assert all(type(v[i][0]) == type(v[i][1]) for v in values if v[i])
