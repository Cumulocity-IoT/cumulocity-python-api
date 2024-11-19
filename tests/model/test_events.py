# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from datetime import timedelta
from unittest.mock import Mock
from urllib.parse import unquote_plus

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Events

from tests.utils import isolate_last_call_arg


def isolate_call_url(fun, **kwargs):
    """Call an Events API function and isolate the request URL for further assertions."""
    c8y = CumulocityApi(base_url='some.host.com', tenant_id='t123', username='user', password='pass')
    c8y.get = Mock(return_value={'events': [], 'statistics': {'totalPages': 1}})
    c8y.delete = Mock(return_value={'events': [], 'statistics': {'totalPages': 1}})
    fun(c8y.events, **kwargs)
    resource = isolate_last_call_arg(c8y.get, 'resource', 0) if c8y.get.called else None
    resource = resource or (isolate_last_call_arg(c8y.delete, 'resource', 0) if c8y.delete.called else None)
    return unquote_plus(resource)


@pytest.mark.parametrize('fun', [
    Events.get_all,
    Events.delete_by,
    Events.get_count,
])
@pytest.mark.parametrize('params, expected, not_expected', [
    ({'expression': 'EX', 'type': 'T'}, ['?EX'], ['type']),
    ({'type': 'T', 'source': 'S', 'fragment': 'F'}, ['type=T', 'source=S', 'fragmentType=F'], []),
    ({'fragment_type': 'T', 'fragment_value': 'V'}, ['fragmentType=T', 'fragmentValue=V'], ['_']),
    ({'status': 'ST', 'severity': 'SE', 'resolved': False}, ['status=ST', 'severity=SE', 'resolved=False'], []),
    ({'reverse': False}, ['revert=False'], ['reverse']),
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
    'fragment_type+fragment_value',
    'status+severity+resolved',
    'reverse',
    'with_source',
    'date_from+date_to',
    'after+before',
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


@pytest.mark.parametrize('fun', [
    Events.get_all,
    Events.get_count,
    Events.delete_by,
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
    (['fragment_value'], ['fragment_type', 'fragment']),
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
    'fragment_value',
    'with_source_assets',
    'with_source_devices',
])
def test_select_invalid_combinations(fun, args, errors):
    """Verify that invalid query filter combinations are raised as expected."""
    with pytest.raises(ValueError) as error:
        params = {x: x.upper() for x in args}
        isolate_call_url(fun, **params)
    assert all(e in str(error) for e in errors)
