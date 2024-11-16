# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

# pylint: disable=protected-access

from __future__ import annotations

import os
from unittest.mock import patch

import jwt
import pytest

from c8y_api._util import c8y_keys
from c8y_api._jwt import JWT
from c8y_api.model._util import _StringUtil


@pytest.mark.parametrize(
    'name, expected',
    [
        ('name', 'name'),
        ('pascal_case', 'pascalCase'),
        ('more_than_one', 'moreThanOne'),
        ('_leading_underscore', 'leadingUnderscore'),
    ])
def test_snake_to_pascal_case(name, expected):
    """Verify that snake case conversion works as expected."""
    assert _StringUtil.to_pascal_case(name) == expected


@patch.dict(os.environ, {'C8Y_SOME': 'some', 'C8Y_THING': 'thing', 'C8YNOT': 'not'}, clear=True)
def test_c8y_keys():
    """Verify that the C8Y_* keys can be filtered from environment."""
    keys = c8y_keys()
    assert len(keys) == 2
    assert 'C8Y_SOME' in keys
    assert 'C8Y_THING' in keys


@pytest.fixture(name='jwt_token_bytes')
def fixture_jwt_token_bytes():
    """Provide a sample JWT token as bytes."""
    payload = {
        'jti': None,
        'iss': 't12345.cumulocity.com',
        'aud': 't12345.cumulocity.com',
        'sub': 'some.user@softwareag.com',
        'tci': '0722ff7b-684f-4177-9614-3b7949b0b5c9',
        'iat': 1638281885,
        'nbf': 1638281885,
        'exp': 1639491485,
        'tfa': False,
        'ten': 't12345',
        'xsrfToken': 'something'}
    return jwt.encode(payload, key='key').encode('utf-8')


def test_resolve_tenant_id(jwt_token_bytes):
    """Verify that parsing the tenant ID from a Bearer authentication
    string works as expected."""
    assert JWT(jwt_token_bytes).tenant_id == 't12345'


def test_resolve_username(jwt_token_bytes):
    """Verify that parsing the username from a Bearer authentication
    string works as expected."""
    assert JWT(jwt_token_bytes).username == 'some.user@softwareag.com'
