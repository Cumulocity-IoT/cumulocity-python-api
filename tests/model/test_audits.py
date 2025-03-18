# Copyright (c) 2025 Cumulocity GmbH

from datetime import timedelta
from unittest.mock import Mock
from urllib.parse import unquote_plus

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import AuditRecords

from tests.utils import isolate_last_call_arg


def isolate_call_url(fun, **kwargs):
    """Call an Applications API function and isolate the request URL for further assertions."""
    c8y = CumulocityApi(base_url='some.host.com', tenant_id='t123', username='user', password='pass')
    c8y.get = Mock(return_value={'auditRecords': [], 'statistics': {'totalPages': 1}})
    c8y.delete = Mock(return_value={'auditRecords': [], 'statistics': {'totalPages': 1}})
    fun(c8y.audit_records, **kwargs)
    resource = isolate_last_call_arg(c8y.get, 'resource', 0) if c8y.get.called else None
    resource = resource or (isolate_last_call_arg(c8y.delete, 'resource', 0) if c8y.delete.called else None)
    return unquote_plus(resource)


@pytest.mark.parametrize('fun', [
    AuditRecords.get_all,
])
@pytest.mark.parametrize('params, expected, not_expected', [
    ({'expression': 'EX', 'type': 'T'}, ['?EX'], ['type']),
    ({'type': 'T', 'name': "it's name", 'owner': 'O', 'user': 'U'},
     ['type=T', "name='it''s name", 'owner=O', 'user=U'],
     []),
    ({'date_from': '2020-12-31', 'date_to': '2021-12-31'},
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
    'type+name+owner+user',
    'date_from+date_to',
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


@pytest.mark.parametrize('fun', [
    AuditRecords.get_all,
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


def test_select_as_tuples():
    """Verify that select as tuples works as expected."""
    changes = [
        {'attribute': 'status11', 'type': 'change'},
        {'attribute': 'status12', 'type': 'change'}
    ]
    jsons = [
        {'id': 'id1', 'severity': 'CRITICAL', 'creationTime': 'time1', 'source': {'id': 'source1'}, 'changes': changes},
        {'id': 'id2', 'severity': 'NORMAL', 'creationTime': 'time2', 'source': {'id': 'source2'}, 'text': 'text'},
    ]
    #
    api = AuditRecords(c8y=Mock())
    api.c8y.get = Mock(side_effect=[{'auditRecords': jsons}, {'auditRecords': []}])
    result = api.get_all(as_tuples=['id', 'creation_time', 'severity', 'source.id', 'changes', 'text'])
    assert result == [
        ('id1', 'time1', 'CRITICAL', 'source1', changes, None),
        ('id2', 'time2', 'NORMAL', 'source2', None, 'text'),
    ]

    api.c8y.get = Mock(side_effect=[{'auditRecords': jsons}, {'auditRecords': []}])
    result = api.get_all(as_tuples=['id', 'creation_time', 'severity', 'source.id', ('changes', []), ('text', '')])
    assert result == [
        ('id1', 'time1', 'CRITICAL', 'source1', changes, ''),
        ('id2', 'time2', 'NORMAL', 'source2', [], 'text'),
    ]

    api.c8y.get = Mock(side_effect=[{'auditRecords': jsons}, {'auditRecords': []}])
    result = api.get_all(as_tuples=('text', '-'))
    assert result == [
        ('-',),
        ('text',),
    ]
