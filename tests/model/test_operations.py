# Copyright (c) 2025 Cumulocity GmbH

from datetime import timedelta
from unittest.mock import Mock
from urllib.parse import unquote_plus

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Operations

from tests.utils import isolate_last_call_arg


def isolate_call_url(fun, **kwargs):
    """Call an Applications API function and isolate the request URL for further assertions."""
    c8y = CumulocityApi(base_url='some.host.com', tenant_id='t123', username='user', password='pass')
    c8y.get = Mock(side_effect=[{'operations': x, 'statistics': {'totalPages': 1}} for x in ([{}], [])])
    c8y.delete = Mock(return_value={'operations': [], 'statistics': {'totalPages': 1}})
    fun(c8y.operations, **kwargs)
    resource = isolate_last_call_arg(c8y.get, 'resource', 0) if c8y.get.called else None
    resource = resource or (isolate_last_call_arg(c8y.delete, 'resource', 0) if c8y.delete.called else None)
    return unquote_plus(resource)


@pytest.mark.parametrize('fun', [
    Operations.get_all,
    Operations.get_last,
    Operations.delete_by
])
@pytest.mark.parametrize('params, expected, not_expected', [
    ({'expression': 'EX', 'type': 'T'}, ['?EX'], ['type']),
    ({'agent_id': 'A', 'device_id': 'D', 'bulk_id': 'B'},
     ['agentId=A', 'deviceId=D', 'bulkOperationId=B'],
     ['_']),
    ({'status': 'S', 'fragment': 'F'},
     ['status=S', 'fragmentType=F'],
     ['fragment=']),
    ({'date_to': '2021-12-31'}, ['dateTo=2021-12-31'], []),
    ({'before': '2021-12-31'}, ['dateTo=2021-12-31'], []),
    ({'min_age': timedelta(days=3), 'max_age': timedelta(weeks=1)},
     ['dateFrom', 'dateTo'],
     ['min', 'max']),
    ({'snake_case': 'SC', 'pascalCase': 'PC'},
     ['snakeCase=SC', 'pascalCase=PC'],
     ['_']),
], ids=[
    'expression',
    'agent+device+bulk',
    'status+fragment',
    'date_to',
    'before',
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
        {'type': 'type1',
         'creationTime': 'time1',
         'deviceId': 'source1',
         'c8y_Command': {'text': 'value1', 'key2': 'value2'}},
        {'type': 'type2',
         'creationTime': 'time2',
         'deviceId': 'source2',
         'c8y_Command': {'text': 'value2'},
         'description': 'description'},
    ]

    api = Operations(c8y=Mock())
    api.c8y.get = Mock(side_effect=[{'operations': jsons}, {'operations': []}])
    result = api.get_all(as_values=[
        'type', 'creation_time', 'c8y_Command.text', 'c8y_Command.key2', 'description'])
    assert result == [
        ('type1', 'time1', 'value1', 'value2', None),
        ('type2', 'time2', 'value2', None, 'description'),
    ]

    api.c8y.get = Mock(side_effect=[{'operations': jsons}, {'operations': []}])
    result = api.get_all(as_values=[
        'type', 'creation_time', 'c8y_Command.text', 'c8y_Command.key2', ('description', '-')])
    assert result == [
        ('type1', 'time1', 'value1', 'value2', '-'),
        ('type2', 'time2', 'value2', None, 'description'),
    ]

    api.c8y.get = Mock(side_effect=[{'operations': jsons}, {'operations': []}])
    result = api.get_all(as_values='type')
    assert result == [
        'type1',
        'type2',
    ]


@pytest.mark.parametrize('fun', [
    Operations.get_all,
    Operations.get_last,
    Operations.delete_by,
])
@pytest.mark.parametrize('args, errors', [
    # date priorities
    (['date_from', 'max_age'], ['date_from', 'after', 'max_age']),
    (['date_to', 'before'], ['date_to', 'before', 'min_age']),
    (['date_to', 'min_age'], ['date_to', 'before', 'min_age']),
], ids=[
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
