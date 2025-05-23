# Copyright (c) 2025 Cumulocity GmbH
import itertools
from datetime import datetime, timedelta
import json
import os
from unittest.mock import Mock, patch
from urllib.parse import unquote_plus

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Measurement, Measurements, Series

from tests.utils import isolate_last_call_arg


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


def test_measurement_parsing_as_values():
    """Verify that parsing Measurements directly as values works as expected."""
    measurements_json = {
        'measurements': [
            {
                'id': '12345',
                'self': 'https://...',
                'type': 'c8y_Measurement',
                'source': {'id': '54321', 'self': 'https://...'},
                'time': '2020-31-12T22:33:44,567Z',
                'c8y_Measurement': {'c8y_temperature': {'unit': 'x', 'value': 12.3}}
            }, {
                'id': '12346',
                'self': 'https://...',
                'type': 'c8y_Measurement',
                'source': {'id': '54321', 'self': 'https://...'},
                'time': '2020-31-12T22:33:44,568Z',
                'c8y_Measurement': {'c8y_temperature': {'unit': 'x', 'value': 34.5}}
            }
        ]
    }
    c8y = CumulocityApi(base_url='base', tenant_id='t12345', username='user', password='pass')
    c8y.get = Mock(side_effect=(measurements_json, {'measurements': []}))
    result = c8y.measurements.get_all(as_values=['id', 'type', 'time', 'c8y_Measurement.c8y_temperature.value'])

    assert result == [
        ('12345', 'c8y_Measurement', '2020-31-12T22:33:44,567Z', 12.3),
        ('12346', 'c8y_Measurement', '2020-31-12T22:33:44,568Z', 34.5),
    ]


def isolate_call_url(fun, **kwargs):
    """Call an Applications API function and isolate the request URL for further assertions."""
    c8y = CumulocityApi(base_url='some.host.com', tenant_id='t123', username='user', password='pass')
    c8y.get = Mock(side_effect=[{'measurements': x, 'statistics': {'totalPages': 1}} for x in ([{}], [])])
    c8y.delete = Mock(return_value={'measurements': [], 'statistics': {'totalPages': 1}})
    with patch('c8y_api.model.Measurement.from_json') as parse_mock:
        parse_mock.return_value = Measurement()
        fun(c8y.measurements, **kwargs)
    resource = isolate_last_call_arg(c8y.get, 'resource', 0) if c8y.get.called else None
    resource = resource or (isolate_last_call_arg(c8y.delete, 'resource', 0) if c8y.delete.called else None)
    return unquote_plus(resource)


@pytest.mark.parametrize('fun', [
    Measurements.get_all,
    Measurements.get_last,
    Measurements.delete_by,
])
@pytest.mark.parametrize('params, expected, not_expected', [
    ({'expression': "X&Y='A''s B'", 'type': 'T'}, ["?X&Y='A''s B'"], ['type']),
    ({'type': 'T', 'source': 'S'},
     ['type=T', 'source=S'],
     []),
    ({'value_fragment_type': 'T', 'value_fragment_series': 'S'},
     ['valueFragmentType=T', 'valueFragmentSeries=S'],
     ['_']),
    ({'series': 'T.S'},
     ['valueFragmentType=T', 'valueFragmentSeries=S'],
     ['series']),
    ({'date_from': '2020-12-31', 'date_to': '2021-12-31'},
     ['dateFrom=2020-12-31', 'dateTo=2021-12-31'],
     []),
    ({'after': '2020-12-31', 'before': '2021-12-31'},
     ['dateFrom=2020-12-31', 'dateTo=2021-12-31'],
     []),
    ({'min_age': timedelta(days=3), 'max_age': timedelta(weeks=1)},
     ['dateFrom', 'dateTo'],
     ['min', 'max']),
    ({'snake_case': 'SC', 'pascalCase': 'PC'},
     ['snakeCase=SC', 'pascalCase=PC'],
     ['_']),
], ids=[
    'expression',
    'type+source',
    'type+series',
    'series',
    'date_from+date_to',
    'after+before',
    'min_age+max_age',
    'kwargs',
])
def test_select(fun, params, expected, not_expected):
    """Verify that the select function's parameters are processed as expected."""
    if fun is Measurements.get_last:
        params = {k: v for k, v in params.items() if k not in ['date_from', 'after', 'max_age']}
        expected = list(filter(lambda x: 'dateFrom' not in x, expected))
    resource = isolate_call_url(fun, **params)
    for e in expected:
        assert e in resource
    for ne in not_expected:
        assert ne not in resource


@pytest.mark.parametrize('fun', [
    Measurements.get_all,
    Measurements.delete_by,
])
@pytest.mark.parametrize('args, errors', [
    # date priorities
    (['date_from', 'after'], ['date_from', 'after', 'max_age']),
    (['date_from', 'max_age'], ['date_from', 'after', 'max_age']),
    (['date_to', 'before'], ['date_to', 'before', 'min_age']),
    (['date_to', 'min_age'], ['date_to', 'before', 'min_age']),
], ids=[
    "date_from+after",
    'date_from+max_age',
    'date_to+before',
    'date_to+min_age',
])
def test_select_invalid_combinations(fun, args, errors):
    """Verify that invalid query filter combinations are raised as expected."""
    with pytest.raises(ValueError) as error:
        params = {x: x.upper() for x in args}
        isolate_call_url(fun, **params)
    assert all(e in str(error) for e in errors)

@pytest.mark.parametrize('params, expected, not_expected', [
    ({'expression': 'X&Y'}, ['X&Y'], ['expression']),
    ({'source': 'SOURCE'}, ['source=SOURCE'], []),
    ({'series': 'SERIES'}, ['series=SERIES'], []),
    ({'series': ['A', 'B']}, ['series=A', 'series=B'], ['source', ',']),
    ({'aggregation': 'A'}, ['aggregationType=A'], ['series=']),
    ({'reverse': True}, ['revert=true'], ['reverse']),
    ({'before': 'BEFORE', 'after': 'AFTER'}, ['dateFrom=AFTER', 'dateTo=BEFORE'], ['source', 'series=']),
    ({'date_from': 'FROM', 'date_to': 'TO'}, ['dateFrom=FROM', 'dateTo=TO'], ['date_to', 'date_from']),
])
def test_get_series_parameters(params, expected, not_expected):
    """Verify that the get_series function parameters are translated as expected."""
    resource = isolate_call_url(Measurements.get_series, **params)
    for e in expected:
        assert e in resource
    for e in not_expected:
        assert e not in resource


def generate_series_data() -> tuple:
    """Generate all kinds of combinations of series fragments.

    Returns a tuple of testcases and corresponding testcase ID. Each testcase
    element is again a tuple of a JSON structure (the test data) and a list
    of expected series' names (for assertion).

    We will define 2 sets (A and B) of such test cases (with different fragment
    names), each featuring possible JSON combinations of single and multiple
    series as well as invalid structures (not following the syntax for series).

    Finally, we will create test cases from all possible combinations of the two
    basic sets.

    The tests' ID are generated from the expectation set.
    """

    def generate(fragment):
        level2_single = ({fragment: {'series1': {'value': 1}}},
                         [f'{fragment}.series1'])
        level2_multi = ({fragment: {'series1': {'value': 1}, 'series2': {'value': 2}}},
                        [f'{fragment}.series1', f'{fragment}.series2'])
        level2_invalid = ({fragment: {'series1': {'data': 1}}},
                          [])
        level2_mix1 = ({fragment: {'series1': {'data': 1}, 'series2': {'value': 2}}},
                       [f'{fragment}.series2'])
        level2_mix2 = ({fragment: {'series1': {'value': 1}, 'series2': {'data': 2}}},
                       [f'{fragment}.series1'])
        return [level2_single, level2_multi, level2_invalid, level2_mix1, level2_mix2]

    # generating A and B sets
    a = generate('fragmentA')
    b = generate('fragmentB')

    # collecting combinations of A and B cases
    ab = [({**r[0][0], **r[1][0]}, r[0][1] + r[1][1]) for r in itertools.product(a, b)]

    cases = a + ab
    # id is the beautified expectation, prefixed with a number
    ids = [f'{i}: ' + ','.join(map(lambda x: x.replace('.', '/'), x[1])) for i, x in enumerate(cases)]

    return cases, ids


@pytest.mark.parametrize('testcase', generate_series_data()[0], ids=generate_series_data()[1])
def test_get_series(testcase):
    """Verify that the get_series function works as expected.

    The `get_series` function on a measurement determines and returns the
    names of series defined within a single measurement.
    """
    data = {**testcase[0], 'source': {'id': '1'}}
    m = Measurement.from_json(data)
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
