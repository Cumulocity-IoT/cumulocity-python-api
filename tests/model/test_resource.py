# Copyright (c) 2025 Cumulocity GmbH

import random
from unittest.mock import Mock
from urllib.parse import urlencode

import pytest

from c8y_api.model import CumulocityResource

from util.testing_util import RandomNameGenerator


def test_build_base_query():
    """Verify that query parameters for object selection are mapped correctly."""
    # pylint: disable=protected-access

    # supported query parameters
    base = RandomNameGenerator.random_name(1)
    kwargs = {
        # some of the below are mapped from python naming
        'type': base + '_type',
        'owner': base + '_owner',
        'source': str(random.randint(1000, 9999)),
        'fragment': base + '_fragment',
        'status': base + '_status',
        'severity': base + '_severity',
        'resolved': 'True',
        'before': base + '_before',
        'after': base + '_after',
        'created_before': base + '_created_after',
        'created_after': base + '_created_after',
        'updated_before': base + '_updated_after',
        'updated_after': base + '_updated_after',
        'reverse': True,
        'page_size': random.randint(0, 10000),
        # random parameters are supported as well and will be mapped 1:1
        'pascalCase': True,
        # snake_case parameters are supported as well and will be translated
        'snake_case': 'value',
    }

    # mapped parameters (python name to API name)
    mapping = {
        'fragment': 'fragmentType',
        'created_before': 'createdTo',
        'created_after': 'createdFrom',
        'updated_before': 'lastUpdatedTo',
        'updated_after': 'lastUpdatedFrom',
        'before': 'dateTo',
        'after': 'dateFrom',
        'reverse': 'revert',
        'page_size': 'pageSize',
        'snake_case': 'snakeCase',
    }

    # expected parameters, kwargs combined with mapping
    expected_params = kwargs.copy()
    for py_key, api_key in mapping.items():
        expected_params[api_key] = expected_params.pop(py_key)
        if isinstance(expected_params[api_key], bool):
            expected_params[api_key] = str(expected_params[api_key]).lower()

    # (1) init mock resource and build query
    resource = CumulocityResource(Mock(), 'res')
    base_query = urlencode(resource._map_params(**kwargs))

    # -> all expected params are there
    for key, value in expected_params.items():
        assert f'{key}={value}' in base_query


@pytest.mark.parametrize('params, expected, not_expected', [
    ({'expression': "X&Y='A''s B'"}, ["?X&Y='A''s%20B'"], []),
    ({'expression': 'X', 'other': 'O'}, ['?X'], ['other', 'O']),
    ({'reverse': True}, ['revert=true'], ['reverse']),
    ({'series': 'A'}, ['series=A'], []),
    ({'series': ['A','B']}, ['series=A', 'series=B'], [',']),
    ({'series': ['A']}, ['series=A'], []),
    ({'ids': [1, 2]}, ['ids=1%2C2'], []),
    ({'before': 'BEFORE', 'after': 'AFTER'}, ['dateFrom=AFTER', 'dateTo=BEFORE'], ['source', 'series=']),
    ({'date_from': 'FROM', 'date_to': 'TO'}, ['dateFrom=FROM', 'dateTo=TO'], ['date_to', 'date_from']),
], ids=[
    'expression',
    'expression+',
    'reverse',
    'series',
    'single series',
    'multi series',
    'ids',
    'before+after',
    'date_from+date_to',
   ])
def test_prepare_query(params, expected, not_expected):
    """Verify that generic query preparation works as expected."""
    resource = CumulocityResource(Mock(), 'res')
    url = resource._prepare_query(**params)
    for e in expected:
        assert e in url
    for e in not_expected:
        assert e not in url
