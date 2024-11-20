# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from unittest.mock import Mock
from urllib.parse import unquote_plus

import pytest

from c8y_api import CumulocityApi
from utils import isolate_last_call_arg


@pytest.mark.parametrize('params, expected, not_expected', [
    ({'expression': 'EX', 'username': 'U'}, ['?EX'], ['username']),
    ({'username': 'U', 'groups': [1, 2, 3], 'owner': 'O'},
     ['username=U', 'groups=1,2,3', 'owner=O'],
     []),
    ({'only_devices': False, 'with_subusers_count': True},
     ['onlyDevices=False', 'withSubusersCount=True'],
     ['_']),
    ({'snake_case': 'SC', 'pascalCase': 'PC'},
     ['snakeCase=SC', 'pascalCase=PC'],
     ['_']),

], ids=[
    'expression',
    'username+groups+owner',
    'only_devices+with_subusers_count',
    'kwargs'
])
def test_select_users(params, expected, not_expected):
    """Verify that user selection parameters are processed as expected."""
    c8y = CumulocityApi(base_url='some.host.com', tenant_id='t123', username='user', password='pass')
    c8y.get = Mock(return_value={'users': [], 'statistics': {'totalPages': 1}})

    c8y.users.get_all(**params)
    resource = isolate_last_call_arg(c8y.get, 'resource', 0) if c8y.get.called else None
    resource = unquote_plus(resource)

    for e in expected:
        assert e in resource
    for ne in not_expected:
        assert ne not in resource
