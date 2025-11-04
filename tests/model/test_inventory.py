# Copyright (c) 2025 Cumulocity GmbH
# pylint: disable=protected-access

from unittest.mock import Mock

import pytest
from urllib import parse

from c8y_api import CumulocityRestApi, CumulocityApi
from c8y_api.model import Inventory, DeviceGroupInventory, DeviceInventory, Binaries
from c8y_api.model._util import _QueryUtil

from tests.utils import isolate_last_call_arg, assert_all_in, assert_all_not_in


@pytest.mark.parametrize('test, expected', [
    ('string', 'string'),
    ('with spaces', 'with spaces'),
    ('quote\'s', 'quote\'\'s')
])
def test_encode_odata_query_value(test, expected):
    """Verify that the query value encoding works as expected."""
    assert _QueryUtil.encode_odata_query_value(test) == expected


@pytest.mark.parametrize('specified, expected', [
    (['query'], ['query']),  # only query
    (['query', 'with'], ['query', 'with']),  # query with flag
    (['query', 'type', 'with'], ['query', 'with']),  # query with flag, type ignored
    (['type', 'with'], ['type', 'with']),
    (['owner', 'with'], ['owner', 'with']),
    (['text', 'with', 'with2'], ['text', 'with', 'with2']),
    (['fragment', 'with'], ['fragment', 'with']),
    (['fragments'], ['query']),  # multiple fragments -> query
    (['fragments', 'with'], ['query', 'with']),
    (['name', 'with'], ['query', 'with']),
    (['fragments', 'fragment'], ['query']),  # only on of fragment/fragments is used
    (['name', 'order_by'], ['query']),  # order_by -> query
    (['fragments', 'name'], ['query']),  # fragments -> query
    (['text', 'name', 'owner', 'type', 'order_by'], ['query']),  # fragments -> query
    (['text', 'owner', 'type', 'fragment'], ['text', 'owner', 'type', 'fragment']),
])
def test_collect_query_params(specified, expected):
    """Verify that query parameters are assembled correctly."""

    kwargs = {x: x.upper() for x in specified}
    params = Inventory._collate_filter_params(only_devices=False, **kwargs)
    assert params.keys() == set(expected)


@pytest.mark.parametrize(
    'kwargs, expected, not_expected',
    [
        ({}, [], ['?', '&']),
        ({'with_children': True}, ['?withChildren=true'], ['&']),
        ({'with_children_count': True, 'with_parents': False},
         ['?', '&', 'withChildrenCount=true', 'withParents=false'], []),
    ],
    ids=[
        'id',
        'single',
        'multiple',
    ]
)
def test_get(kwargs, expected, not_expected):
    """Verify that inventory's get function works as expected."""
    c8y: CumulocityRestApi = Mock()
    c8y.get = Mock(return_value={'managedObjects': []})

    inventory = Inventory(c8y)
    inventory.get('12345', **kwargs)

    assert c8y.get.call_count == 1
    url = parse.unquote_plus(isolate_last_call_arg(c8y.get, 'resource', 0))
    assert_all_in(['/12345', *expected], url)
    assert_all_not_in(not_expected, url)


@pytest.mark.parametrize('name, expected', [
    ('some name', 'query=$filter=(name eq \'some name\')'),
    ('some\'s name', 'query=$filter=(name eq \'some\'\'s name\')')
])
def test_select_by_name(name, expected):
    """Verify that the inventory's select function can filter by name."""

    # In the end, the select function should result in a GET request; the
    # result of this is not important, we simulate an empty result set.
    c8y: CumulocityRestApi = Mock()
    c8y.get = Mock(return_value={'managedObjects': []})

    inventory = Inventory(c8y)
    inventory.get_all(name=name)

    assert c8y.get.call_count == 1
    url = parse.unquote_plus(isolate_last_call_arg(c8y.get, 'resource', 0))
    assert expected in url


def test_select_by_name_plus():
    """Verify that the inventory's select function will put all filters
    as parts of a complex query."""

    c8y: CumulocityRestApi = Mock()
    c8y.get = Mock(return_value={'managedObjects': []})

    inventory = Inventory(c8y)
    inventory.get_all(name='NAME', fragment='FRAGMENT', type='TYPE', owner='OWNER')

    # we expect that the following strings are part of the resource string
    expected = [
        'query=',
        'has(FRAGMENT)',
        'name eq \'NAME\'',
        'owner eq OWNER',
        'type eq TYPE']

    assert c8y.get.call_count == 1
    url = parse.unquote_plus(isolate_last_call_arg(c8y.get, 'resource', 0))

    for e in expected:
        assert e in url


@pytest.mark.parametrize('inventory_class', [Inventory, DeviceInventory, DeviceGroupInventory, Binaries])
def test_select_as_values(inventory_class):
    """Verify that select as values works as expected."""
    data = [
        {'name': 'n1', 'type': 't1', 'test_Fragment': {'key': 'value1', 'key2': 'value2'}},
        {'name': 'n2', 'type': 't2', 'test_Fragment': {'key': 'value2'}},
    ]
    c8y: CumulocityRestApi = Mock()

    inventory = inventory_class(c8y)
    c8y.get = Mock(side_effect=[{'managedObjects': data}, {'managedObjects': []}])
    result = inventory.get_all(as_values=['name', 'type', 'test_Fragment.key', 'test_Fragment.key2'])
    assert result == [
        ('n1', 't1', 'value1', 'value2'),
        ('n2', 't2', 'value2', None),
    ]

    c8y.get = Mock(side_effect=[{'managedObjects': data}, {'managedObjects': []}])
    result = inventory.get_all(as_values=['name', 'type', 'test_Fragment.key', ('test_Fragment.key2', '-')])
    assert result == [
        ('n1', 't1', 'value1', 'value2'),
        ('n2', 't2', 'value2', '-'),
    ]


def _invoke_target_and_isolate_url(target, kwargs):
    """Auxiliary function to invoke a HTTP target function (by name) on a
    fake CumulocityApi instance and return the URL (resource) of a get call."""
    c8y: CumulocityApi = CumulocityApi('base', 'tenant', 'user', 'pass')
    c8y.get = Mock(return_value={'managedObjects': [], 'statistics': {'totalPages': 1}})
    api, fun = target.split('.', maxsplit=1)
    getattr(getattr(c8y, api), fun)(**kwargs)
    return parse.unquote_plus(isolate_last_call_arg(c8y.get, 'resource', 0))


def gen_common_select_cases():
    """Generate test case data for common select cases."""
    return [
        # expression has the highest priority
        ({'expression': 'EX', 'query': 'QUERY'}, ['?EX'], ['query=', 'QUERY']),
        ({'expression': 'EX', 'ids': [1, 2, 3]}, ['?EX'], ['ids=']),
        ({'expression': 'EX', 'name': 'NAME'}, ['?EX'], ['name=', 'NAME']),
        # query has 2nd highest priority
        ({'query': 'QUERY', 'ids': [1, 2, 3]}, ['=QUERY'], ['ids=']),
        # ids has 3rd highest priority
        ({'ids': [1, 2, 3], 'name': 'NAME'}, ['ids=1,2,3'], ['name=', 'NAME']),
        # any of parent, name, order_by, and fragments triggers "query mode"
        ({'name': "it's name"},
         ['$filter=', "name eq 'it''s name'"],
         ['name=']),
        ({'name': 'N', 'fragment': 'F'},
         ['$filter=', "name eq 'N'", 'has(F)'],
         ['fragment=', 'name=']),
        ({'parent': 'P', 'fragment': 'F'},
         ['$filter=', "bygroupid(P)", 'has(F)'],
         ['fragment=', 'name=', 'parent=']),
        ({'order_by': 'a asc', 'type': 'T'},
         ['$filter=', "type eq T", '$orderby=a asc'],
         ['order_by', 'orderBy', 'type=']),
        ({'fragments': ['a', 'b'], 'owner': 'O', 'text': "it's text"},
         ['$filter=', 'has(a)', 'has(b)', 'owner eq O', "text eq 'it''s text'"],
         ['fragment=', 'owner=']),
        # otherwise, simple filters should be used as such
        # (fragment filters are special and tested per API below)
        ({'owner': 'O'}, ['owner=O'], ['$', ' eq ']),
        ({'type': 'T'}, ['type=T'], ['$', ' eq ']),
        ({'text': "it's text"}, ["text='it''s text'"], ['$', ' eq ']),
        # other flags don't change the query mode
        # (fragment filters are special and tested per API below)
        ({'type': 'T', 'with_children': False}, ['type=T', 'withChildren=false'], ['$', 'has']),
        ({'owner': 'O', 'skip_children_names': False}, ['owner=O', 'skipChildrenNames=false'], ['$', 'has']),
        ({'text': "it's text", 'with_latest_values': True},
         ["text='it''s text'", 'withLatestValues=true'],
         ['$', ' eq ']),
        ({'name': "it's name", 'with_groups': False}, ["name eq 'it''s name'", 'withGroups=false'], ['name=']),
        # test all kinds of known parameters
        ({'with_children_count': False}, ['withChildrenCount=false'], ['with_children_count']),
        ({'skip_children_names': False}, ['skipChildrenNames=false'], ['skip_children_names']),
        ({'with_groups': False}, ['withGroups=false'], ['with_groups']),
        ({'with_parents': False}, ['withParents=false'], ['with_parents']),
        ({'with_latest_values': False}, ['withLatestValues=false'], ['with_latest_values']),
        ({'any_other_param': False}, ['anyOtherParam=false'], ['any_other_param']),
        ({'pascalCaseParam': 12}, ['pascalCaseParam=12'], []),
    ]


def gen_common_select_cases_ids():
    """Generate test case ID for common select cases."""
    return ['+'.join(x[0].keys()) for x in gen_common_select_cases()]


@pytest.mark.parametrize('params, expected, not_expected',
                         gen_common_select_cases(),
                         ids=gen_common_select_cases_ids())
@pytest.mark.parametrize('fun', [
        'inventory.get_all',
        'inventory.get_count',
        'device_inventory.get_all',
        'device_inventory.get_count',
        'group_inventory.get_all',
        'group_inventory.get_count',
])
def test_common_select_params(fun, params, expected, not_expected):
    """Verify that common select parameters are handled correctly."""
    url = _invoke_target_and_isolate_url(fun, params)
    for e in expected:
        assert e in url
    for e in not_expected:
        assert e not in url


# ({'fragment': 'ANY'}, ['fragmentType='], ['$', 'has']),
# ({'fragment': 'F', 'only_roots': True}, ['fragmentType=', 'onlyRoots=True'], ['$', 'has']),
@pytest.mark.parametrize('fun',[
        'device_inventory.get_all',
        'device_inventory.get_count',
    ])
@pytest.mark.parametrize('params, expected, not_expected', [
    ({}, ['fragmentType=c8y_IsDevice'], ['q=', '$filter', 'has']),
    ({'fragment': 'F'}, ['q=', '$filter', 'has(c8y_IsDevice)', 'has(F)'], ['fragmentType=']),
    ({'fragments': ['F1', 'F2']}, ['q=', '$filter', 'has(c8y_IsDevice)', 'has(F1)', 'has(F2)'], ['fragmentType=']),
    ({'name': "it's name"}, ['q=', "name eq 'it''s name'"], ['query']),
], ids=[
    'unfiltered',
    'fragment',
    'fragments',
    'name',
])
def test_device_inventory_filters(fun, params, expected, not_expected):
    """Verify that the filter parameters are all forwarded correctly
    end-to-end through all abstract helper methods."""
    url = _invoke_target_and_isolate_url(fun, params)
    for e in expected:
        assert e in url
    for e in not_expected:
        assert e not in url


@pytest.mark.parametrize('fun', [
        'group_inventory.get_all',
        'group_inventory.get_count',
    ])
@pytest.mark.parametrize('params, expected, not_expected', [
    ({},
     ['fragmentType=c8y_IsDeviceGroup'],
     ['type']),
    ({'fragment': 'F'},
     ['query=', '$filter', 'has(c8y_IsDeviceGroup)', 'has(F)'],
     ['fragmentType=']),
    ({'fragments': ['F1', 'F2']},
     ['query=', '$filter', 'has(c8y_IsDeviceGroup)', 'has(F1)', 'has(F2)'],
     ['fragmentType=']),
    ({'parent': 'PARENT'},
     ['query=', 'bygroupid(PARENT)', 'type eq c8y_DeviceSubGroup'],
     []),
    ({'parent': 'PARENT', 'name': "it's name"},
     ['query=', "name eq 'it''s name'", 'bygroupid(PARENT)', 'type eq c8y_DeviceSubGroup'],
     []),
    # query invalidates all other parameters
    ({'parent': 'PARENT', 'name': "it's name", 'query': 'QUERY'},
     ['query=QUERY'],
     ['name eq ', 'bygroupid', 'PARENT', 'c8y_DeviceSubGroup']),
], ids=[
    'unfiltered',
    'fragment',
    'fragments',
    'parent',
    'name+parent',
    'name+parent+query',
])
def test_group_inventory_filters(fun, params, expected, not_expected):
    """Verify that the filter parameters are all forwarded correctly
    end-to-end through all abstract helper methods."""
    url = _invoke_target_and_isolate_url(fun, params)
    for e in expected:
        assert e in url
    for e in not_expected:
        assert e not in url
