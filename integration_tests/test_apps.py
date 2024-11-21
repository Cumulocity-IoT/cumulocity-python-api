# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

import pytest
import requests

from c8y_api import CumulocityRestApi, HTTPBearerAuth
from c8y_api.app import SimpleCumulocityApp, MultiTenantCumulocityApp
from c8y_api.model import ManagedObject

from tests.utils import build_auth_string, b64encode


@pytest.fixture(name='token_app')
def fix_token_app(test_environment):
    """Provide a token-based REST API instance."""
    # First, create an instance for basic auth
    c8y = SimpleCumulocityApp()
    # Submit auth request
    form_data = {
            'grant_type': 'PASSWORD',
            'username': c8y.auth.username,
            'password': c8y.auth.password
        }
    r = requests.post(url=c8y.base_url + '/tenant/oauth', data=form_data, timeout=60.0)
    # Obtain token from response
    assert r.status_code == 200
    cookie = r.headers['Set-Cookie']
    # split by ; to separate parts, then map a=b items to dictionary
    cookie_parts = {x[0]: x[1] for x in [c.split('=') for c in cookie.split(';')] if len(x) == 2}
    auth_token = cookie_parts['authorization']
    assert auth_token
    # build token-based app
    return CumulocityRestApi(
        base_url=c8y.base_url,
        tenant_id=c8y.tenant_id,
        auth=HTTPBearerAuth(auth_token)
    )


def test_token_based_app_headers(token_app):
    """Verify that a token-based app only features a 'Bearer' auth header."""
    response = token_app.session.get("https://httpbin.org/headers")
    auth_header = response.json()['headers']['Authorization']
    assert auth_header.startswith('Bearer')


def test_token_based_app(token_app):
    """Verify that a token-based app can be used for all kind of requests."""
    mo = ManagedObject(token_app, name='test-object', type='test-object-type').create()
    mo['new_Fragment'] = {}
    mo.update()
    mo.delete()


def test_oai_secure_login():
    """Verify that a cookies from an OAI-Secure login are parsed correctly."""
    # First, create an instance for basic auth
    c8y = SimpleCumulocityApp()

    # (1) Submit auth request
    form_data = {
            'grant_type': 'PASSWORD',
            'username': c8y.auth.username,
            'password': c8y.auth.password
        }
    response = requests.post(url=c8y.base_url + '/tenant/oauth', data=form_data, timeout=60.0)
    # -> should be ok
    assert response.status_code == 200
    # -> should contain cookies
    assert 'Set-Cookie' in response.headers
    assert 'authorization' in response.cookies
    assert 'XSRF-TOKEN' in response.cookies

    # (2) build an OAI-based request
    # Cumulocity forwards the cookies as well as the XSRF token _and_
    # a phony Basic Auth header which contains tenant ID and username
    # with a fake password to ensure backwards compatibility.
    request = requests.Request(
        method="GET",
        url="any",
        cookies=response.cookies,
        headers={
            'Accept': 'application/json',
            'Authorization': build_auth_string(b64encode(f'{c8y.tenant_id}/{c8y.username}:<fake password>')),
            'X-Xsrf-Token': response.cookies['XSRF-TOKEN']
        }
    )
    # -> user scope instance can be obtained
    c8y_user = c8y.get_user_instance(request.headers, request.cookies)
    assert c8y_user.username == c8y.username
    assert isinstance(c8y_user.auth, HTTPBearerAuth)
    assert c8y_user.auth.token == response.cookies['authorization']


def test_context_manager(test_environment):
    """Verify that the Apps can be used as context managers."""

    with SimpleCumulocityApp() as c8y:
        assert c8y.username

    with MultiTenantCumulocityApp() as c8y:
        assert c8y.bootstrap_instance.username
