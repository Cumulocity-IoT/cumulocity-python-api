# Copyright (c) 2025 Cumulocity GmbH

from unittest.mock import Mock
from urllib.parse import unquote_plus

import pytest

from c8y_api import CumulocityApi, CumulocityRestApi
from c8y_api.model import Users
from tests.utils import isolate_last_call_arg


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


def test_select_as_values():
    """Verify that select as values works as expected."""
    jsons = [
        {'userName': 'user1',
         'enabled': True,
         'applications': [],
         'customProperties': {'p1': 'v1', 'p2': 'v2'}},
        {'userName': 'user2',
         'enabled': False,
         'applications': [{'a': 1}, {'b': 2}],
         'customProperties': {'p1': 'v2'},
         'phone': '+123'},
    ]

    c8y = CumulocityRestApi(base_url='base', tenant_id='t123', username='u', password='p')
    api = Users(c8y)
    api.c8y.get = Mock(side_effect=[{'users': jsons}, {'users': []}])
    result = api.get_all(as_values=[
        'user_name', 'enabled', 'applications', 'customProperties.p1', 'customProperties.p2', 'phone'])
    assert result == [
        ('user1', True, [], 'v1', 'v2', None),
        ('user2', False, [{'a': 1}, {'b': 2}], 'v2', None, '+123'),
    ]

    c8y = CumulocityRestApi(base_url='base', tenant_id='t123', username='u', password='p')
    api = Users(c8y)
    api.c8y.get = Mock(side_effect=[{'users': jsons}, {'users': []}])
    result = api.get_all(as_values=[
        'userName', 'enabled', 'applications', 'custom_properties.p1', ('customProperties.p2', 'v3'), ('phone', '')])
    assert result == [
        ('user1', True, [], 'v1', 'v2', ''),
        ('user2', False, [{'a': 1}, {'b': 2}], 'v2', 'v3', '+123'),
    ]

    c8y = CumulocityRestApi(base_url='base', tenant_id='t123', username='u', password='p')
    api = Users(c8y)
    api.c8y.get = Mock(side_effect=[{'users': jsons}, {'users': []}])
    result = api.get_all(as_values='enabled')
    assert result == [
        True,
        False,
    ]
