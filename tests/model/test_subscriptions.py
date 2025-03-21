# Copyright (c) 2025 Cumulocity GmbH

from unittest.mock import Mock
from urllib.parse import unquote_plus

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Subscriptions

from tests.utils import isolate_last_call_arg


def isolate_call_url(fun, **kwargs):
    """Call an Applications API function and isolate the request URL for further assertions."""
    c8y = CumulocityApi(base_url='some.host.com', tenant_id='t123', username='user', password='pass')
    c8y.get = Mock(return_value={'subscriptions': [], 'statistics': {'totalPages': 1}})
    c8y.delete = Mock(return_value={'subscriptions': [], 'statistics': {'totalPages': 1}})
    fun(c8y.notification2_subscriptions, **kwargs)
    resource = isolate_last_call_arg(c8y.get, 'resource', 0) if c8y.get.called else None
    resource = resource or (isolate_last_call_arg(c8y.delete, 'resource', 0) if c8y.delete.called else None)
    return unquote_plus(resource)


@pytest.mark.parametrize('fun', [
    Subscriptions.get_all,
    Subscriptions.get_count,
])
@pytest.mark.parametrize('params, expected, not_expected', [
    ({'expression': 'EX', 'context': 'C'}, ['?EX'], ['context']),
    ({'context': 'C', 'source': 'S', 'subscription': 'SU', 'type_filter': 'F'},
     ['context=C', 'source=S', 'subscription=SU', 'typeFilter=F'],
     []),
    ({'snake_case': 'SC', 'pascalCase': 'PC'},
     ['snakeCase=SC', 'pascalCase=PC'],
     ['_']),
], ids=[
    'expression',
    'context+source+subscription+type_filter',
    'kwargs'
])
def test_select(fun, params, expected, not_expected):
    """Verify that the select function's parameters are processed as expected."""
    resource = isolate_call_url(fun, **params)
    for e in expected:
        assert e in resource
    for ne in not_expected:
        assert ne not in resource
