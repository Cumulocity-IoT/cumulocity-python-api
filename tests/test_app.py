# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

import base64
import os
import time
from unittest import mock
from unittest.mock import patch, Mock

import pytest
import responses

from requests.auth import HTTPBasicAuth, AuthBase

from c8y_api import CumulocityApi
from c8y_api.app import SimpleCumulocityApp, MultiTenantCumulocityApp, _CumulocityAppBase
from c8y_api._auth import HTTPBearerAuth, AuthUtil
from c8y_api._jwt import JWT

from tests.utils import b64encode, build_auth_string, sample_jwt, isolate_last_call_arg


env_per_tenant = {
    'C8Y_BASEURL': 'http://baseurl',
    'C8Y_TENANT': 'tenant_id',
    'C8Y_USER': 'tenant_user',
    'C8Y_PASSWORD': 'tenant_password',
    'APPLICATION_KEY': 'application_key',
}

env_multi_tenant = {
    'C8Y_BASEURL': 'http://baseurl',
    'C8Y_BOOTSTRAP_TENANT': 'tenant_id',
    'C8Y_BOOTSTRAP_USER': 'tenant_user',
    'C8Y_BOOTSTRAP_PASSWORD': 'tenant_password',
    'APPLICATION_KEY': 'application_key',
}


def build_basic_auth(user, password) -> dict:
    """Create an auth header for testing."""
    return {'Authorization': 'Basic ' +
            base64.b64encode(f'{user}:{password}'.encode('utf-8')).decode('utf-8')}


@mock.patch.dict(os.environ, env_per_tenant, clear=True)
def test_per_tenant():
    """Verify that the instance will be created properly within a
    per-tenant environment"""

    c8y = SimpleCumulocityApp()

    # -> instance was initialized with environment variables
    assert c8y.tenant_id == env_per_tenant['C8Y_TENANT']
    assert c8y.username == env_per_tenant['C8Y_USER']
    assert isinstance(c8y.auth, HTTPBasicAuth)
    assert c8y.auth.password == env_per_tenant['C8Y_PASSWORD']
    assert c8y.application_key == env_per_tenant['APPLICATION_KEY']

    # -> requests will be prepended with the base url
    with responses.RequestsMock() as rsps:
        rsps.add(method='GET',
                 url=env_per_tenant['C8Y_BASEURL'] + '/xyz',
                 status=200,
                 json={})
        c8y.get('/xyz')


@mock.patch.dict(os.environ, env_per_tenant, clear=True)
def test_per_tenant__user_instances():
    """Verify that user instances are correctly instantiated (and cached)
    for single-tenant apps."""

    c8y = SimpleCumulocityApp(
        processing_mode='proc-mode',
        cache_ttl=2,
    )

    # create user instance from HTTP headers
    user1, pass1 = ('user1', 'pw1')
    headers1 = build_basic_auth(user1, pass1)

    c8y_1 = c8y.get_user_instance(headers1)

    # -> instance is correctly initialized
    assert c8y_1.tenant_id == c8y.tenant_id
    assert c8y_1.username == user1
    assert isinstance(c8y_1.auth, HTTPBasicAuth)
    assert c8y_1.auth.password == pass1

    # -> properties are inherited
    assert c8y_1.application_key == c8y.application_key
    assert c8y_1.processing_mode == c8y.processing_mode

    # requesting an instance for the same user
    c8y_2 = c8y.get_user_instance(headers1)

    # -> cached instance is returned
    assert c8y_2 is c8y_1

    # wait for cache timeout and request again
    time.sleep(2)
    c8y_3 = c8y.get_user_instance(headers1)

    # -> new instance is created
    assert c8y_3 is not c8y_1


@mock.patch.dict(os.environ, env_per_tenant, clear=True)
def test_per_tenant_application_key():
    """Verify that the application key from environment can be overwritten."""

    # manually setting application
    c8y = SimpleCumulocityApp(application_key='app_key')
    # -> application key from environment overridden
    assert c8y.application_key == 'app_key'

    # create user instance from HTTP headers
    headers = build_basic_auth('user', 'pass')
    c8y_user = c8y.get_user_instance(headers)
    # -> application key inherited
    assert c8y_user.application_key == c8y.application_key


@mock.patch.dict(os.environ, env_multi_tenant, clear=True)
def test_multi_tenant__bootstrap_instance():
    """Verify that the bootstrap instance will be created properly within a
    multi-tenant environment."""

    c8y = MultiTenantCumulocityApp(
        # application_key='app-key',
        processing_mode='proc-mode',
    ).bootstrap_instance

    # -> bootstrap instance is initialized with environment variables
    assert c8y.tenant_id == env_multi_tenant['C8Y_BOOTSTRAP_TENANT']
    assert c8y.username == env_multi_tenant['C8Y_BOOTSTRAP_USER']
    assert isinstance(c8y.auth, HTTPBasicAuth)
    assert c8y.auth.password == env_multi_tenant['C8Y_BOOTSTRAP_PASSWORD']
    assert c8y.application_key == env_multi_tenant['APPLICATION_KEY']
    # -> properties are inherited
    assert c8y.processing_mode == 'proc-mode'

    # -> requests will be prepended with the base url
    with responses.RequestsMock() as rsps:
        rsps.add(method='GET',
                 url=env_multi_tenant['C8Y_BASEURL'] + '/xyz',
                 status=200,
                 json={})
        c8y.get('/xyz')


@mock.patch.dict(os.environ, env_multi_tenant, clear=True)
def test__multi_tenant__user_instances():
    """Verify that user instances are correctly instantiated (and cached)
    for multi-tenant apps."""

    c8y = MultiTenantCumulocityApp(
        application_key='app-key',
        processing_mode='proc-mode',
        cache_ttl=2,
    )

    # create user instance from HTTP headers
    tenant1, user1, pass1 = ('t1', 'user1', 'pw1')
    headers1 = build_basic_auth(f'{tenant1}/{user1}', pass1)

    c8y_1 = c8y.get_user_instance(headers1)

    # -> instance is correctly initialized
    assert c8y_1.tenant_id == tenant1
    assert c8y_1.username == user1
    assert isinstance(c8y_1.auth, HTTPBasicAuth)
    assert c8y_1.auth.password == pass1

    # -> properties are inherited
    assert c8y_1.application_key == c8y.application_key
    assert c8y_1.processing_mode == c8y.processing_mode

    # requesting an instance for the same user
    c8y_2 = c8y.get_user_instance(headers1)

    # -> cached instance is returned
    assert c8y_2 is c8y_1

    # wait for cache timeout and request again
    time.sleep(2)
    c8y_3 = c8y.get_user_instance(headers1)

    # -> new instance is created
    assert c8y_3 is not c8y_1


@mock.patch.dict(os.environ, env_multi_tenant, clear=True)
def test_multi_tenant__caching_instances():
    """Verify that instances are cached by their tenant ID and the cache
    is evaluated properly."""
    # pylint: disable=protected-access

    # prepare a mock instance cache
    get_auth_mock = Mock(return_value=HTTPBasicAuth('username', 'password'))

    c8y_factory = MultiTenantCumulocityApp(
        application_key='app-key',
        processing_mode='proc-mode',
        cache_ttl=2,
    )
    c8y_factory._get_tenant_auth = get_auth_mock

    # (1) get a specific instance
    c8y = c8y_factory.get_tenant_instance('t12345')
    # -> auth was read
    get_auth_mock.assert_called_with('t12345')
    # -> the instance is cached
    assert 't12345' in c8y_factory._tenant_instances
    # -> attributes reflect what's in the mock cache
    assert c8y.tenant_id == 't12345'
    assert c8y.username == 'username'
    assert isinstance(c8y.auth, HTTPBasicAuth)
    assert c8y.auth.password == 'password'
    # properties are inherited
    assert c8y.application_key == c8y_factory.application_key
    assert c8y.processing_mode == c8y_factory.processing_mode

    # (2) let's do that again
    get_auth_mock.reset_mock()
    c8y2 = c8y_factory.get_tenant_instance('t12345')
    # -> auth was not read again
    get_auth_mock.assert_not_called()
    # -> the instance is just the same
    assert c8y2 is c8y

    # (3) let's wait for the TTL to pass and try again
    get_auth_mock.reset()
    time.sleep(2)
    c8y3 = c8y_factory.get_tenant_instance('t12345')
    # -> auth was read again
    get_auth_mock.assert_called_with('t12345')
    # -> the instance is a new one
    assert c8y3 is not c8y


@mock.patch.dict(os.environ, env_multi_tenant, clear=True)
def test_multi_tenant__build_from_subscriptions():
    """Verify that an uncached instance is build using the subscriptions."""
    # pylint: disable=protected-access

    with patch.object(MultiTenantCumulocityApp, '_read_subscription_auths') as read_subscriptions:
        # we mock _read_subscriptions so that we don't need an actual
        # connection and have a proper return value
        read_subscriptions.return_value = {'t12345': HTTPBasicAuth('username', 'password')}

        c8y_factory = MultiTenantCumulocityApp()
        c8y = c8y_factory.get_tenant_instance('t12345')

        # -> subscriptions have been read
        read_subscriptions.assert_called()
        # -> subscriptions are cached
        assert 't12345' in c8y_factory._subscribed_auths
        # -> instance is now in cache
        assert 't12345' in c8y_factory._tenant_instances
        # -> attributes reflect was returned by the subscriptions mock
        assert c8y.tenant_id == 't12345'
        assert c8y.username == 'username'
        assert isinstance(c8y.auth, HTTPBasicAuth)
        assert c8y.auth.password == 'password'
        assert c8y.application_key == c8y_factory.application_key

        # using the same tenant ID again
        read_subscriptions.reset_mock()
        c8y2 = c8y_factory.get_tenant_instance('t12345')
        # -> this will be the exact same instance
        assert c8y2 is c8y
        # -> subscriptions are not read again
        read_subscriptions.assert_not_called()

        # clearing tenant from cache and reading again
        del c8y_factory._tenant_instances['t12345']
        c8y3 = c8y_factory.get_tenant_instance('t12345')
        # -> tenant instance is build again
        assert c8y3 is not c8y
        # -> subscriptions are not read again because they are cached
        read_subscriptions.assert_not_called()


@mock.patch.dict(os.environ, env_multi_tenant, clear=True)
def test_multi_tenant__override_application_key():
    """Verify that the application key from environment can be overwritten."""

    # manually setting application
    c8y = MultiTenantCumulocityApp(application_key='app_key')
    # -> application key from environment overridden
    assert c8y.application_key == 'app_key'

    # create user instance from HTTP headers
    headers = build_basic_auth('t12345/user', 'pass')
    c8y_user = c8y.get_user_instance(headers)
    # -> application key inherited
    assert c8y_user.application_key == c8y.application_key

    # create tenant instance
    with patch.object(MultiTenantCumulocityApp, '_read_subscription_auths') as read_subscriptions:
        # we mock _read_subscriptions so that we don't need an actual
        # connection and have a proper return value
        read_subscriptions.return_value = {'t12345': HTTPBasicAuth('username', 'password')}
        c8y_tenant = c8y.get_tenant_instance('t12345')
        assert c8y_tenant.application_key == c8y.application_key


@mock.patch.dict(os.environ, env_multi_tenant, clear=True)
def test_read_subscriptions():
    """Verify that the subscriptions are read and parsed properly."""
    # pylint: disable=protected-access

    mock_response = {'users': [
        {'tenant': 't12345',
         'name': 'user12345',
         'password': 'pass12345'},
        {'tenant': 't54321',
         'name': 'user54321',
         'password': 'pass54321'},
    ]}

    base_url = 'https://base.com'
    tenant_id = 't0'
    user = 'user'
    password = 'pass'

    with responses.RequestsMock() as rsps:
        # we want to ensure that exactly this is called
        rsps.add(method='GET',
                 url=base_url + '/application/currentApplication/subscriptions',
                 status=200,
                 json=mock_response)

        # we just need any CumulocityApi to do this call
        c8y = CumulocityApi(base_url=base_url, tenant_id=tenant_id, username=user, password=password)
        subscriptions = MultiTenantCumulocityApp._read_subscription_auths(c8y)
        # -> subscriptions were parsed correctly
        assert 't12345' in subscriptions
        assert 't54321' in subscriptions
        assert subscriptions['t12345'].password == 'pass12345'
        assert subscriptions['t54321'].username == 't54321/user54321'


@mock.patch.dict(os.environ, env_multi_tenant, clear=True)
@pytest.mark.parametrize('auth_value, tenant_id', [
    (b64encode('t12345/some@domain.com:password'), 't12345'),
    (sample_jwt(sub='someuser@domain.com', ten='t543'), 't543'),
])
def test_get_tenant_instance_from_headers(auth_value, tenant_id):
    """Verify that the authorization header is parsed and passed correctly
    when the tenant ID is resolved from the request headers."""
    # pylint: disable=protected-access

    c8y_factory = MultiTenantCumulocityApp()

    # we intercept all calls to the internal _get function
    c8y_factory._get_tenant_instance = Mock()

    # request a tenant instance from header dict
    c8y_factory.get_tenant_instance(headers={'authorization': build_auth_string(auth_value)})
    # -> the get function is called with the tenant ID
    c8y_factory._get_tenant_instance.assert_called_once_with(tenant_id)

@mock.patch.dict(os.environ, env_multi_tenant, clear=True)
def test_get_tenant_instance_missing_auth_info():
    """Verify that missing auth information from headers or cookies is
    reported correctly."""
    c8y_factory = MultiTenantCumulocityApp()

    # missing auth information in headers
    with pytest.raises(KeyError) as e:
        c8y_factory.get_tenant_instance(headers={'random': 'header'})
    assert 'random' in str(e)

    # missing auth information in cookies
    with pytest.raises(KeyError) as e:
        c8y_factory.get_tenant_instance(cookies={'random': 'cookie'})
    assert 'random' in str(e)

    # missing auth information in headers and cookies
    with pytest.raises(KeyError) as e:
        c8y_factory.get_tenant_instance(headers={'header': '1'}, cookies={'cookie': '2'})
    assert 'header' in str(e) and 'cookie' in str(e)


@pytest.mark.parametrize('auth_value, username', [
    (b64encode('t12345/some@domain.com:password'), 't12345/some@domain.com'),
    (b64encode('someuser@domain.com:password'), 'someuser@domain.com'),
    (sample_jwt(sub='someuser@domain.com', ten='t12345'), 'someuser@domain.com'),
])
def test_get_user_instance(auth_value, username):
    """Verify that a user instance is obtained from inbound HTTP headers."""
    # pylint: disable=protected-access

    c8y = _CumulocityAppBase(log=Mock())
    # we intercept calls to the _build function
    c8y._build_user_instance = Mock()

    # build a user instance
    c8y.get_user_instance(headers={'Authorization': build_auth_string(auth_value)})
    # -> _build was called with a proper auth
    call_arg = isolate_last_call_arg(c8y._build_user_instance, 'auth', 0)
    assert isinstance(call_arg, AuthBase)
    # -> username and tenant_id should be correct
    if isinstance(call_arg, HTTPBasicAuth):
        assert call_arg.username == username
    if isinstance(call_arg, HTTPBearerAuth):
        assert JWT(call_arg.token).username == username


@pytest.mark.parametrize('cookies, username', [
    ({'authorization': sample_jwt(sub='someuser@domain.com', ten='t12345')}, 'someuser@domain.com'),
    ({'foo': 'bar', 'authorization': sample_jwt(sub='someuser@domain.com', ten='t12345')}, 'someuser@domain.com'),
    ({'authorization': sample_jwt(sub='someuser@domain.com', ten='t12345'), 'foo': 'bar'}, 'someuser@domain.com'),
])
def test_get_user_instance_cookie(cookies, username):
    """Verify that a user instance is obtained from inbound HTTP headers with cookies."""
    # pylint: disable=protected-access

    c8y = _CumulocityAppBase(log=Mock())
    # we intercept calls to the _build function
    c8y._build_user_instance = Mock()

    # build a user instance
    c8y.get_user_instance(cookies=cookies)
    # -> _build was called with a proper auth
    call_arg = isolate_last_call_arg(c8y._build_user_instance, 'auth', 0)
    assert isinstance(call_arg, HTTPBearerAuth)
    # -> username and tenant_id should be correct
    assert JWT(call_arg.token).username == username


@mock.patch.dict(os.environ, env_per_tenant, clear=True)
@pytest.mark.parametrize('auth_value, username', [
    (b64encode('t555/some@domain.com:password'), 'some@domain.com'),
    (b64encode('someuser@domain.com:password'), 'someuser@domain.com'),
    (sample_jwt(sub='someuser@domain.com', ten='t543'), 'someuser@domain.com'),
])
def test_build_user_instance(auth_value, username):
    """Verify that a user instance can be created from a proper set of
    inbound HTTP headers."""
    # pylint: disable=protected-access

    # build an Auth instance from the auth value
    auth = AuthUtil.parse_auth_string(build_auth_string(auth_value))

    with patch.dict(os.environ, env_per_tenant, clear=True):
        c8y_app = SimpleCumulocityApp()
        user_c8y = c8y_app._build_user_instance(auth)
        # -> the tenant ID matches the parent
        assert user_c8y.tenant_id == c8y_app.tenant_id
        # -> the username is parsed from the auth
        assert user_c8y.username == username
