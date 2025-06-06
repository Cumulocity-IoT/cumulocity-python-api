# Copyright (c) 2025 Cumulocity GmbH

from datetime import timedelta
from unittest.mock import Mock
from urllib.parse import unquote_plus

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Alarms, Alarm

from tests.utils import isolate_last_call_arg


def isolate_call_url(fun, **kwargs):
    """Call an Alarms API function and isolate the request URL for further assertions."""
    c8y = CumulocityApi(base_url='some.host.com', tenant_id='t123', username='user', password='pass')
    c8y.get = Mock(return_value={'alarms': [], 'statistics': {'totalPages': 1}})
    c8y.put = Mock(return_value={'alarms': [], 'statistics': {'totalPages': 1}})
    c8y.delete = Mock(return_value={'alarms': [], 'statistics': {'totalPages': 1}})
    if fun is Alarms.apply_by:
        fun(c8y.alarms, Alarm(), **kwargs)
    else:
        fun(c8y.alarms, **kwargs)
    resource = isolate_last_call_arg(c8y.get, 'resource', 0) if c8y.get.called else None
    resource = resource or (isolate_last_call_arg(c8y.put, 'resource', 0) if c8y.put.called else None)
    resource = resource or (isolate_last_call_arg(c8y.delete, 'resource', 0) if c8y.delete.called else None)
    return unquote_plus(resource)


@pytest.mark.parametrize('fun', [
    Alarms.get_all,
    Alarms.count,
    Alarms.apply_by,
    Alarms.delete_by,
])
@pytest.mark.parametrize('params, expected, not_expected', [
    ({'expression': 'EX', 'type': 'T'}, ['?EX'], ['type']),
    ({'type': 'T', 'source': 'S', 'fragment': 'F'}, ['type=T', 'source=S', 'fragmentType=F'], []),
    ({'status': 'ST', 'severity': 'SE', 'resolved': False}, ['status=ST', 'severity=SE', 'resolved=False'], []),
    ({'reverse': False, 'page_size': 8}, ['revert=false', 'pageSize=8'], ['reverse']),
    ({'with_source_assets': False, 'with_source_devices': True, 'source': '123'},
     ['withSourceAssets=False', 'withSourceDevices=True'],
     ['_']),
    # data priorities
    ({'date_from': '2020-12-31', 'date_to': '2021-12-31'},
     ['dateFrom=2020-12-31', 'dateTo=2021-12-31'],
     []),
    ({'after': '2020-12-31', 'before': '2021-12-31'},
     ['dateFrom=2020-12-31', 'dateTo=2021-12-31'],
     []),
    ({'created_from': '2020-12-31', 'created_to': '2021-12-31'},
     ['createdFrom=2020-12-31', 'createdTo=2021-12-31'],
     []),
    ({'last_updated_from': '2020-12-31', 'last_updated_to': '2021-12-31'},
     ['lastUpdatedFrom=2020-12-31', 'lastUpdatedTo=2021-12-31'],
     []),
    ({'min_age': timedelta(days=3), 'max_age': timedelta(weeks=1)},
     ['dateFrom', 'dateTo'],
     ['min', 'max']),
    ({'snake_case': 'SC', 'pascalCase': 'PC'},
     ['snakeCase=SC', 'pascalCase=PC'],
     ['_']),

], ids=[
    'expression',
    'type+source+fragment',
    'status+severity+resolved',
    'reverse+page_size',
    'with_source',
    'date_from+date_to',
    'after+before',
    'created_from+created_to',
    'last_updated_from+last_updated_to',
    'min_age+max_age',
    'kwargs'
])
def test_select(fun, params, expected, not_expected):
    """Verify that the select function's parameters are processed as expected."""
    resource = isolate_call_url(fun, **params)
    for e in expected:
        assert e in resource
    for ne in not_expected:
        assert ne not in resource


def test_select_as_values():
    """Verify that select as values works as expected."""
    jsons = [
        {'type': 'type1', 'text': 'text1', 'source': 'source1', 'test_Fragment': {'key': 'value1', 'key2': 'value2'}},
        {'type': 'type2', 'text': 'text2', 'source': 'source2', 'test_Fragment': {'key': 'value2'}},
    ]

    api = Alarms(c8y=Mock())
    api.c8y.get = Mock(side_effect=[{'alarms': jsons}, {'alarms': []}])
    result = api.get_all(as_values=['type', 'text', 'test_Fragment.key', 'test_Fragment.key2'])
    assert result == [
        ('type1', 'text1', 'value1', 'value2'),
        ('type2', 'text2', 'value2', None),
    ]

    api.c8y.get = Mock(side_effect=[{'alarms': jsons}, {'alarms': []}])
    result = api.get_all(as_values=['type', 'text', 'test_Fragment.key', ('test_Fragment.key2', '-')])
    assert result == [
        ('type1', 'text1', 'value1', 'value2'),
        ('type2', 'text2', 'value2', '-'),
    ]


@pytest.mark.parametrize('fun', [
    Alarms.get_all,
    Alarms.count,
    Alarms.apply_by,
    Alarms.delete_by,
])
@pytest.mark.parametrize('args, errors', [
    # date priorities
    (['date_from', 'after'], ['date_from', 'after', 'max_age']),
    (['date_from', 'max_age'], ['date_from', 'after', 'max_age']),
    (['date_to', 'before'], ['date_to', 'before', 'min_age']),
    (['date_to', 'min_age'], ['date_to', 'before', 'min_age']),
    (['created_from', 'created_after'], ['created_from', 'created_after']),
    (['created_to', 'created_before'], ['created_to', 'created_before']),
    (['last_updated_from', 'updated_after'], ['last_updated_from', 'updated_after']),
    (['last_updated_to', 'updated_before'], ['last_updated_to', 'updated_before']),
    (['with_source_assets'], ['source']),
    (['with_source_devices'], ['source']),
], ids=[
    "date_from+after",
    'date_from+max_age',
    'date_to+before',
    'date_to+min_age',
    'created_from+created_before',
    'created_to+created_after',
    'updated_from+updated_before',
    'updated_to+updated_after',
    'with_source_assets',
    'with_source_devices',
])
def test_select_invalid_combinations(fun, args, errors):
    """Verify that invalid query filter combinations are raised as expected."""
    with pytest.raises(ValueError) as error:
        params = {x: x.upper() for x in args}
        isolate_call_url(fun, **params)
    assert all(e in str(error) for e in errors)
