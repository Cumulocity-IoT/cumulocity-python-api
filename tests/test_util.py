# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

# pylint: disable=protected-access

from __future__ import annotations

import os
import time
from unittest.mock import patch

import jwt
import pytest

from c8y_api._util import c8y_keys, validate_base_url
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


@pytest.mark.parametrize('path', ['/', '/some/path', ''])
@pytest.mark.parametrize('port', [':80', ''])
@pytest.mark.parametrize('host', ['host.com', 'some.host.com'])
@pytest.mark.parametrize('scheme', ['https://', 'http://', ''])
def test_validate_base_url(scheme, host, port, path):
    """Verify that the base URL validation works with all potential URL format combinations."""
    url = scheme + host + port + path
    url2 = validate_base_url(url)
    assert url2 == (scheme or 'https://') + host + port


def create_jwt_token(tenant_id, hostname, username, valid_seconds=60) -> str:
    """Create a dummy JWT token as string."""
    payload = {
        'jti': None,
        'iss': hostname,
        'aud': hostname,
        'sub': username,
        'tci': '0722ff7b-684f-4177-9614-3b7949b0b5c9',
        'iat': int(time.time()),
        'nbf': int(time.time()),
        'exp': int(time.time()) + valid_seconds,
        'tfa': False,
        'ten': tenant_id,
        'xsrfToken': 'something',
    }
    return jwt.encode(payload, key='key')


@pytest.fixture(name='jwt_token')
def fixture_jwt_token() -> str:
    """Provide a sample JWT token as string."""
    return create_jwt_token('t12345', 't12345.cumulocity.com', 'some.user@softwareag.com')


@pytest.fixture(name='jwt_token_bytes')
def fixture_jwt_token_bytes(jwt_token) -> bytes:
    """Provide a sample JWT token as bytes."""
    return jwt_token.encode('utf-8')


def test_resolve_tenant_id(jwt_token_bytes):
    """Verify that parsing the tenant ID from a Bearer authentication
    string works as expected."""
    assert JWT(jwt_token_bytes).tenant_id == 't12345'


def test_resolve_username(jwt_token_bytes):
    """Verify that parsing the username from a Bearer authentication
    string works as expected."""
    assert JWT(jwt_token_bytes).username == 'some.user@softwareag.com'
