# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

import pytest
from requests.auth import HTTPBasicAuth

from c8y_api._jwt import JWT
from c8y_api._auth import AuthUtil, HTTPBearerAuth
from tests.utils import b64encode, sample_jwt, build_auth_string


# @pytest.mark.parametrize('auth_value, username', [
#     (b64encode('t12345/some@domain.com:password'), 'some@domain.com'),
#     (sample_jwt(sub='someuser@domain.com', ten='t12345'), 'someuser@domain.com'),
# ])
# def test_resolve_username(auth_value: str, username: str):
#     assert username == AuthUtil.resolve_username_from_auth_string(build_auth_string(auth_value))
#
#
# @pytest.mark.parametrize('auth_value, tenant_id', [
#     (b64encode('t12345/some@domain.com:password'), 't12345'),
#     (sample_jwt(sub='someuser@domain.com', ten='t12345'), 't12345'),
# ])
# def test_resolve_tenant(auth_value: str, tenant_id: str):
#     """Verify that parsing of an Basic authentication string works as expected."""
#     assert tenant_id == AuthUtil.resolve_tenant_id_from_auth_string(build_auth_string(auth_value))
#
#
# @pytest.mark.parametrize('auth_value, hint', [
#     (b64encode('some@domain.com:password'), 'tenant'),
#     (sample_jwt(sub='someuser@domain.com'), 'tenant'),
# ])
# def test_resolve_tenant_basic_bad(auth_value: str, hint: str):
#     """Verify that parsing of an Basic authentication string works as expected."""
#     with pytest.raises(ValueError) as e:
#         AuthUtil.resolve_tenant_id_from_auth_string(build_auth_string(auth_value))
#     assert hint in str(e)
#
#
@pytest.mark.parametrize('auth_value, tenant_id', [
    (b64encode('t12345/some@domain.com:password'), 't12345'),
    (sample_jwt(sub='someuser@domain.com', ten='t12345'), 't12345'),
])
def test_get_tenant_id(auth_value, tenant_id):
    """Verify that the tenant ID can be resolved from any Auth instance."""
    auth = AuthUtil.parse_auth_string(build_auth_string(auth_value))
    assert AuthUtil.get_tenant_id(auth) == tenant_id


@pytest.mark.parametrize('auth_value', [
    b64encode('some@domain.com:password'),
    sample_jwt(sub='someuser@domain.com', ten=''),
])
def test_get_tenant_id_bad(auth_value):
    """Verify that a missing tenant ID in the authorization information
    results in a ValueError."""
    auth = AuthUtil.parse_auth_string(build_auth_string(auth_value))
    with pytest.raises(Exception):
        AuthUtil.get_tenant_id(auth)


@pytest.mark.parametrize('auth_value, username', [
    (b64encode('t12345/some@domain.com:password'), 't12345/some@domain.com'),
    (b64encode('someone@domain.com:password'), 'someone@domain.com'),
    (sample_jwt(sub='someuser@domain.com', ten='t12345'), 'someuser@domain.com'),
])
def test_get_username(auth_value, username):
    """Verify that the username can be resolved from any Auth instance."""
    auth = AuthUtil.parse_auth_string(build_auth_string(auth_value))
    assert AuthUtil.get_username(auth) == username


def test_parse_auth_basic():
    """Verify that a BASIC authentication string can be parsed."""
    auth_value = b64encode('t123/some@domain.com:password')

    auth1 = AuthUtil.parse_basic_auth_value(auth_value)
    assert auth1.username == 't123/some@domain.com'

    auth2 = AuthUtil.parse_auth_string(build_auth_string(auth_value))
    assert isinstance(auth2, HTTPBasicAuth)
    assert auth2.username == auth1.username


def test_parse_auth_bearer():
    """Verify that a BEARER authentication string can be parsed."""
    auth_value = sample_jwt(ten='t543', sub='someuser@domain.com')

    auth1 = AuthUtil.parse_bearer_auth_value(auth_value)
    jwt1 = JWT(auth1.token)
    assert jwt1.tenant_id == 't543'
    assert jwt1.username == 'someuser@domain.com'

    auth2 = AuthUtil.parse_auth_string(build_auth_string(auth_value))
    assert isinstance(auth2, HTTPBearerAuth)
    jwt2 = JWT(auth2.token)
    assert jwt2.tenant_id == jwt1.tenant_id
    assert jwt2.username == jwt1.username
