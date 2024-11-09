# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

import os
from unittest.mock import patch, Mock

import pytest
import requests

from _jwt import JWT
from c8y_tk.interactive import CumulocityContext
from test_util import create_jwt_token


def build_auth_response(auth_cookie):
    r = requests.Response()
    r.status_code = 200
    r.cookies = {'authorization': auth_cookie, 'XSRF-TOKEN': 'XSRF'}
    return r


@pytest.fixture(name='token')
def fix_token(request):
    valid_seconds = request.param if request else None
    hostname = 'tenant.cumulocity.com'
    tenant_id = 't12345'
    username = 'some@user.com'
    return create_jwt_token(tenant_id, hostname, username, valid_seconds)


def test_with_token(monkeypatch):
    """Verify that a valid token is obtained from the environment."""

    hostname = 'tenant.cumulocity.com'
    tenant_id = 't12345'
    username = 'some@user.com'
    token = create_jwt_token(tenant_id, hostname, username, 60*60+10)

    monkeypatch.setenv('C8Y_TOKEN', token)
    with CumulocityContext() as c8y:
        assert c8y.base_url == f'https://{hostname}'
        assert c8y.username == username
        assert c8y.tenant_id == tenant_id

    assert os.environ['C8Y_TOKEN'] == token


@pytest.mark.parametrize('token', [60*60-1], indirect=True, ids=['Expired'])
@patch('c8y_api._base_api.requests.post')
@patch('c8y_tk.interactive.context.getpass.getpass')
def test_with_invalid_token(getpass_fun, post_fun, monkeypatch, token: str):
    """Verify that an invalid token is revoked and a new token is generated.

    All necessary information to generate a new token should be taken from
    the previous token, except for the password which is requested from
    the user (interactively).
    """

    # build a new, now valid token
    old_token = JWT(token)
    new_token = create_jwt_token(
        tenant_id=old_token.tenant_id,
        hostname=old_token.get_claim('iss'),
        username=old_token.username,
        valid_seconds=60*60,
    )

    # inject old token into environment
    monkeypatch.setenv('C8Y_TOKEN', token)
    # assume getpass function to be invoked
    getpass_fun.return_value = 'some-password'
    # assume post function to be invoked
    post_fun.return_value = build_auth_response(new_token)

    with CumulocityContext() as c8y:
        # new CumulocityApi instance should use new token
        assert c8y.auth.token is new_token

    # provided password should have been used to renew token
    post_args = post_fun.call_args.kwargs
    assert post_args['data']['username'] == old_token.username
    assert post_args['data']['password'] == 'some-password'

    # new token should be written to environment
    assert os.environ['C8Y_TOKEN'] is new_token


@patch('c8y_api._base_api.requests.post')
@patch('c8y_tk.interactive.context.getpass.getpass')
@patch('c8y_tk.interactive.context.input')
def test_without_token(input_fun, getpass_fun, post_fun, monkeypatch):
    """Verify that all necessary information is requested from the user if
    no environment variables are defined."""

    token = create_jwt_token('some-tenant', 'some.host.com', username='some-user')

    # ensure that we don't have any environment variables
    monkeypatch.delenv('C8Y_TOKEN', raising=False)
    monkeypatch.delenv('C8Y_BASEURL', raising=False)
    monkeypatch.delenv('C8Y_TENANT', raising=False)
    monkeypatch.delenv('C8Y_USER', raising=False)
    monkeypatch.delenv('C8Y_PASSWORD', raising=False)

    # mock calls to input and getpass
    input_fun.return_value = 'some-value'
    getpass_fun.return_value = 'some-password'
    # assume post function to be invoked
    post_fun.return_value = build_auth_response(token)

    with CumulocityContext() as c8y:
        assert c8y.username == 'some-user'
        assert c8y.tenant_id == 'some-tenant'
        assert c8y.base_url == 'https://some.host.com'
        assert c8y.auth.token == token

    # verify prompts
    #  - collect (lowercase) prompts from all calls
    #  - ensure that keywords are mentioned in prompts
    all_prompts = [x.args[0].lower() for x in input_fun.call_args_list]
    assert all(any(x in prompt for prompt in all_prompts) for x in ['tenant', 'username', 'hostname'])

    # provided username and password should have been used to build token
    post_args = post_fun.call_args.kwargs
    assert post_args['data']['username'] == 'some-value'
    assert post_args['data']['password'] == 'some-password'


@pytest.mark.parametrize('have_password', [True, False], ids=['Pass', ''])
@pytest.mark.parametrize('have_username', [True, False], ids=['User', ''])
@pytest.mark.parametrize('have_tenant_id', [True, False], ids=['Tenant', ''])
@pytest.mark.parametrize('have_base_url', [True, False], ids=['URL', ''])
@patch('c8y_api._base_api.requests.post')
@patch('c8y_tk.interactive.context.getpass.getpass')
@patch('c8y_tk.interactive.context.input')
def test_environment_use(input_fun, getpass_fun, post_fun,
                         have_base_url, have_tenant_id, have_username, have_password):
    """Verify that standard C8Y_* environment variables are used when provided."""
    # (1) ensure environment
    have_vars = [
        ('baseurl', have_base_url),
        ('tenant', have_tenant_id),
        ('user', have_username),
        ('password', have_password)
    ]
    env = {f'C8Y_{var.upper()}': f'env-{var}' for var, have in have_vars if have}

    # mock calls to input and getpass
    input_fun.return_value = 'some-value'
    getpass_fun.return_value = 'some-password'
    # assume post function to be invoked
    token = create_jwt_token('some-tenant', 'some.host.com', username='some-user')
    post_fun.return_value = build_auth_response(token)
    # ensure no passwords are cached
    CumulocityContext._cached_passwords.clear()

    # request connection with given environment
    with patch.dict('os.environ', env, clear=True) as patched_env:
        with CumulocityContext() as c8y:
            assert c8y.auth.token == token
            # token should be written back to environment
            assert patched_env['C8Y_TOKEN'] == token
            # other environment variables should not have been touched
            assert all(f'C8Y_{var.upper()}' in patched_env for var, have in have_vars if have)
            assert all(f'C8Y_{var.upper()}' not in patched_env for var, have in have_vars if not have)

    # verify authentication parameters
    post_args = post_fun.call_args.kwargs
    assert ('env-baseurl' if have_base_url else 'some-value') in post_args['url']
    assert ('tenant_id=env-tenant' if have_tenant_id else 'tenant_id=some-value') in post_args['url']
    assert post_args['data']['username'] == ('env-user' if have_username else 'some-value')
    assert post_args['data']['password'] == ('env-password' if have_password else 'some-password')

    # new token should be written to environment
    assert os.environ['C8Y_TOKEN'] == token


@patch('c8y_api._base_api.requests.post')
@patch('c8y_tk.interactive.context.getpass.getpass')
# @patch.dict('os.environ', {}, clear=True)
def test_dont_ask_password_again(getpass_fun, post_fun):
    """Verify that a password is requested from a user only once.

    This test has 2 phases:
       1) build connection from token info and password from user
       2) build connection from token info and cached password
    """

    old_token = create_jwt_token('some-tenant', 'some.host.com', username='some-user', valid_seconds=-1)
    new_token = create_jwt_token('some-tenant', 'some.host.com', username='some-user', valid_seconds=60*60)

    # mock calls to getpass
    getpass_fun.return_value = 'some-password'
    # assume post function to be invoked
    post_fun.return_value = build_auth_response(new_token)

    # ensure no passwords are cached
    CumulocityContext._cached_passwords.clear()

    # request connection with given environment
    with patch.dict('os.environ', {}, clear=True) as patched_env:
        patched_env['C8Y_TOKEN'] = old_token

        with CumulocityContext() as c8y:
            assert c8y.auth.token == new_token

        # password should have been read from user
        getpass_fun.assert_called()

        # switch back to invalid token and reset mock
        patched_env['C8Y_TOKEN'] = old_token
        getpass_fun.reset_mock()

        with CumulocityContext() as c8y:
            assert c8y.auth.token == new_token

        getpass_fun.assert_not_called()


@patch('c8y_api._base_api.requests.post')
@patch('c8y_tk.interactive.context.getpass.getpass')
# @patch.dict('os.environ', {}, clear=True)
def test_refresh_password_on_auth_failure(getpass_fun, post_fun):
    """Verify that a password is requested from a user only once.

    This test has 2 phases:
       1) build connection from token info and password from user
       2) build connection from token info and cached password
    """

    old_token = create_jwt_token('some-tenant', 'some.host.com', username='some-user', valid_seconds=-1)
    new_token = create_jwt_token('some-tenant', 'some.host.com', username='some-user', valid_seconds=60*60)

    # mock calls to getpass
    getpass_fun.return_value = 'some-password'

    # build mock auth (failed) response
    response = Mock(requests.Response)
    response.url = 'http://some.host.com'
    response.status_code = 401
    response.json = Mock(return_value={"message": ""})
    post_fun.side_effect = [response, build_auth_response(new_token)]

    # inject a mock password
    CumulocityContext._cached_passwords['some-user'] = 'wrong-password'

    # request connection with given environment
    with patch.dict('os.environ', {'C8Y_TOKEN': old_token}, clear=True) as patched_env:

        with CumulocityContext() as c8y:
            assert c8y.auth.token == new_token

            # password should have been read from user (once)
            getpass_fun.assert_called_once()

    assert CumulocityContext._cached_passwords['some-user'] == 'some-password'


@patch('c8y_api._base_api.requests.post')
@patch('c8y_tk.interactive.context.getpass.getpass')
@patch('c8y_tk.interactive.context.input')
# @patch.dict('os.environ', {}, clear=True)
def test_tfa_support(input_fun, getpass_fun, post_fun):
    """Verify that a password is requested from a user only once.

    This test has 2 phases:
       1) build connection from token info and password from user
       2) build connection from token info and cached password
    """

    old_token = create_jwt_token('some-tenant', 'some.host.com', username='some-user', valid_seconds=-1)
    new_token = create_jwt_token('some-tenant', 'some.host.com', username='some-user', valid_seconds=60*60)

    # mock calls to getpass
    getpass_fun.return_value = 'some-password'
    input_fun.return_value = 'some-tfa'

    # build mock auth (failed) response
    response = Mock(requests.Response)
    response.url = 'http://some.host.com'
    response.status_code = 401
    response.json = Mock(return_value={'message': 'TFA'})
    post_fun.side_effect = [response, build_auth_response(new_token)]

    CumulocityContext._cached_passwords['some-user'] = 'good-password'

    # request connection with given environment
    with patch.dict('os.environ', {'C8Y_TOKEN': old_token}, clear=True) as patched_env:

        with CumulocityContext() as c8y:
            assert c8y.auth.token == new_token

            # password should have been read from user (once)
            getpass_fun.assert_not_called()
            input_fun.assert_called_once()

    # verify authentication parameters of 2nd call
    assert len(post_fun.call_args_list) == 2
    post_args = post_fun.call_args.kwargs
    assert post_args['data']['tfa_token'] == 'some-tfa'
    assert post_args['data']['password'] == 'good-password'
