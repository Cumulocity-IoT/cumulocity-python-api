# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

import json
import os
from unittest.mock import Mock

import pytest

from c8y_api import CumulocityRestApi
from c8y_api.model.tenant_options import TenantOption, TenantOptions
from tests.utils import isolate_last_call_arg


@pytest.fixture(scope='function', name='sample_json')
def fix_sample_json() -> dict:
    """Provide sample JSON data."""
    path = os.path.dirname(__file__) + '/tenant_option.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        return json.load(f)


def test_parsing(sample_json):
    """Verify that parsing a Tenant Option from JSON works."""
    option = TenantOption.from_json(sample_json)

    assert option.category == sample_json['category']
    assert option.key == sample_json['key']
    assert option.value == sample_json['value']


@pytest.fixture(scope='function', name='sample_option')
def fix_sample_option() -> TenantOption:
    """Provide a sample object for various tests."""
    return TenantOption(category='some.category', key='credentials.some_key', value='some value')


def test_full_formatting(sample_option: TenantOption):
    """Verify that full JSON formatting works."""
    option_json = sample_option.to_full_json()

    assert 'self' not in option_json
    assert len(option_json.keys()) == 3

    assert option_json['category'] == sample_option.category
    assert option_json['key'] == sample_option.key
    assert option_json['value'] == sample_option.value


def test_diff_formatting(sample_option: TenantOption):
    """Verify that diff JSON formatting works."""
    # update all fields (only value counts)
    sample_option.value = 'new value'
    sample_option.category = 'new.category'
    sample_option.key = 'new.key'

    option_json = sample_option.to_diff_json()

    # only the value can effectively be updated,
    # the rest will not be part of the JSON
    assert list(option_json.keys()) == ['value']
    option_json['value'] = sample_option.value


def test_create(sample_json: dict, sample_option: TenantOption):
    """Verify that object creation works as expected.

    Calling the `create` function should render the option using `to_json`
    and invoke the `post` function on the underlying CumulocityRestApi
    instance accordingly.
    """

    # we create a mock for the `c8y` connection, the `post` function
    # in particular which should be invoked
    c8y: CumulocityRestApi = Mock()
    c8y.post = Mock(return_value=sample_json)
    sample_option.c8y = c8y

    # we need to control the to_json function
    sample_option.to_json = Mock(return_value={'expected': True})

    updated_option = sample_option.create()

    # 1) to_json should have been called
    assert sample_option.to_json.call_count == 1
    # 2) the given resource path should be correct
    resource = isolate_last_call_arg(c8y.post, 'resource', 0)
    assert resource == '/tenant/options'
    # 3) the given payload should match what to_json returned
    payload = isolate_last_call_arg(c8y.post, 'json', 1)
    assert set(payload.keys()) == {'expected'}
    # 4) the return should be parsed properly
    assert updated_option.key == sample_json['key']
    assert updated_option.category == sample_json['category']
    assert updated_option.value == sample_json['value']


def test_update(sample_json: dict, sample_option: TenantOption):
    """Verify that object update works as expected.

    Calling the `update` function should render the option using `to_json`
    and invoke the `put` function on the underlying CumulocityRestApi
    instance accordingly.
    """

    # creating a mock for the `c8y` connection,
    # controlling the `put` function (which return a proper response)
    c8y: CumulocityRestApi = Mock()
    c8y.put = Mock(return_value=sample_json)

    sample_option.to_json = Mock(return_value={'expected': True})

    sample_option.c8y = c8y
    sample_option.value = 'new value'
    result = sample_option.update()

    # 1) to_json should have been called
    assert sample_option.to_json.call_count == 1
    only_updated = isolate_last_call_arg(sample_option.to_json, 'only_updated', 0)
    assert only_updated
    # 2) the given resource path should be correct
    resource = isolate_last_call_arg(c8y.put, 'resource', 0)
    assert resource == f'/tenant/options/{sample_option.category}/{sample_option.key}'
    # 3) the given payload should match what to_json returned
    payload = isolate_last_call_arg(c8y.put, 'json', 1)
    assert set(payload.keys()) == {'expected'}
    # 4) the response should be parsed
    assert result.key == sample_json['key']
    assert result.category == sample_json['category']
    assert result.value == sample_json['value']


def test_select_by_category(sample_json: dict):
    """Verify that selection by category works as expected.

    Calling `get_all` will invoke `select` and convert the result to a list.
    This should lead to two calls to the underlying `get` function. Also,
    the result should be parsed properly.
    """
    c8y: CumulocityRestApi = Mock()
    c8y.get = Mock(return_value={'key1': 'value1', 'key2': 'value2'})

    tos = TenantOptions(c8y)
    result = tos.get_all(category='some.category')

    # the get function should have been called 1 time as
    # select by category does not iterate
    assert c8y.get.call_count == 1
    url = isolate_last_call_arg(c8y.get, 'resource', 0)
    assert '/tenant/options/some.category' in url
    # the result should have been parsed
    assert len(result) == 2
    assert result[0].category == 'some.category'
    assert result[0].key == 'key1'
    assert result[0].value == 'value1'
    assert result[1].key == 'key2'
    assert result[1].value == 'value2'


def test_update_by_category():
    """Verify that update by category works as expected.

    Calling `update_by` will invoke the underlying `put` function with a
    specific resource string and payload.
    """

    c8y: CumulocityRestApi = Mock()
    c8y.put = Mock()

    tos = TenantOptions(c8y)
    update_info = {'k1': 'v1', 'k2': 'v2'}
    tos.update_by(category='some.category', options=update_info)

    # put should have been called just once
    assert c8y.put.call_count == 1
    # the resource should specify the category
    url = isolate_last_call_arg(c8y.put, 'resource', 0)
    assert url == '/tenant/options/some.category'
    # the payload should match the given data
    assert isolate_last_call_arg(c8y.put, 'json', 1) == update_info
