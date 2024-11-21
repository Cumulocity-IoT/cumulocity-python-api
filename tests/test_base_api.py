# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

# pylint: disable=protected-access, redefined-outer-name

import base64
from unittest.mock import patch

import json
import pytest
import requests
import responses

from c8y_api._base_api import (
    CumulocityRestApi,
    ProcessingMode,
    UnauthorizedError,
    AccessDeniedError,
    HttpError
)


@pytest.fixture(scope='function')
def mock_c8y() -> CumulocityRestApi:
    """Provide mock CumulocityRestApi instance."""
    return CumulocityRestApi(
        base_url='http://base.com',
        tenant_id='t12345',
        username='username',
        password='password',
        application_key='application_key',
        processing_mode=ProcessingMode.QUIESCENT,
    )


@pytest.fixture(scope='module')
def httpbin_basic() -> CumulocityRestApi:
    """Provide mock CumulocityRestApi instance for httpbin with basic auth."""
    return CumulocityRestApi(
        base_url='https://httpbin.org',
        tenant_id='t12345',
        username='username',
        password='password'
    )


def assert_auth_header(c8y, headers):
    """Assert that the given auth header is correctly formatted."""
    auth_header = headers['Authorization'].lstrip('Basic ')
    pw = c8y.auth.password
    expected = f'{c8y.tenant_id}/{c8y.username}:{pw}'
    assert base64.b64decode(auth_header) == expected.encode('utf-8')


def assert_accept_header(headers, accept='application/json'):
    """Assert that the accept header matches the expectation."""
    assert headers['Accept'] == accept


def assert_content_header(headers, content_type='application/json'):
    """Assert that the content-type header matches the expectation."""
    assert headers['Content-Type'] == content_type


def assert_application_key_header(c8y, headers):
    """Assert that the application key header matches the expectation."""
    assert headers[c8y.HEADER_APPLICATION_KEY] == c8y.application_key


def assert_processing_mode_header(c8y, headers):
    """Assert that the processing mode header matches the expectation."""
    assert headers[c8y.HEADER_PROCESSING_MODE] == c8y.processing_mode


@pytest.mark.parametrize(
    'tenant_id, username, password, expected_username',
    [
        ('t123', 'user', 'pass', 'user'),
        ('t123', 't123/user', 'pass', 'user'),
    ])
def test_username_parsing(tenant_id, username, password, expected_username):
    """Verify that usernames are parsed as expected."""
    c8y = CumulocityRestApi(base_url="some.host.com", tenant_id=tenant_id, username=username, password=password)
    assert c8y.username == expected_username
    assert c8y.auth.username == f'{tenant_id}/{expected_username}'


@pytest.mark.parametrize('args, expected', [
    ({'accept': 'application/json'}, {'Accept': 'application/json'}),
    ({'content_tYPe': 'content/TYPE'}, {'Content-Type': 'content/TYPE'}),
    ({'some': 'thing', 'mORE_Of_this': 'same'}, {'Some': 'thing', 'More-Of-This': 'same'}),
    ({'empty': None, 'accept': 'accepted'}, {'Accept': 'accepted'}),
    ({'empty1': None, 'empty2': None}, None),
    ({'accept': ''}, {'Accept': None}),
])
def test_prepare_headers(args, expected):
    """Verify header preparation."""
    assert CumulocityRestApi._prepare_headers(**args) == expected


@pytest.mark.parametrize('method', ['get', 'post', 'put'])
def test_remove_accept_header(mock_c8y: CumulocityRestApi, method):
    """Verify that the default accept header can be unset/removed."""

    with responses.RequestsMock() as rsps:
        rsps.add(method=method.upper(),
                 url=mock_c8y.base_url + '/resource',
                 status=200,
                 json={})
        kwargs = {'resource': '/resource', 'accept': ''}
        if method.startswith('p'):
            kwargs['json'] = {}

        func = getattr(mock_c8y, method)
        func(**kwargs)

        assert 'Accept' not in rsps.calls[0].request.headers


@pytest.mark.online
@pytest.mark.parametrize('method', ['get', 'post', 'put'])
def test_remove_accept_header_online(httpbin_basic: CumulocityRestApi, method):
    """Verify that the unset accept header are actually not sent."""

    kwargs = {'resource': '/anything', 'accept': ''}
    if method.startswith('p'):
        kwargs['json'] = {}

    func = getattr(httpbin_basic, method)
    response = func(**kwargs)

    assert 'Accept' not in response['headers']


@pytest.mark.parametrize('method', ['get', 'post', 'put', 'delete'])
def test_no_application_key_header(mock_c8y: CumulocityRestApi, method):
    """Verify that the application key header is not present by default."""

    c8y = CumulocityRestApi(mock_c8y.base_url, mock_c8y.tenant_id, mock_c8y.username, mock_c8y.username)

    with responses.RequestsMock() as rsps:
        rsps.add(method=method.upper(),
                 url=mock_c8y.base_url + '/resource',
                 status=200,
                 json={'result': True})

        kwargs = {'resource': '/resource'}
        if method.startswith('p'):
            kwargs['json'] = {}

        func = getattr(c8y, method)
        if method.startswith('p'):
            kwargs.update({'json': {}})

        func(**kwargs)
        request_headers = rsps.calls[0].request.headers

        assert CumulocityRestApi.HEADER_APPLICATION_KEY not in request_headers


@pytest.mark.online
def test_basic_auth_get(httpbin_basic: CumulocityRestApi):
    """Verify that the basic auth headers are added for the REST requests."""
    c8y = httpbin_basic

    # first we verify that the auth is there for GET requests
    response = c8y.get('/anything')
    assert_auth_header(c8y, response['headers'])


def test_post_defaults(mock_c8y: CumulocityRestApi):
    """Verify the basic functionality of the POST requests."""

    with responses.RequestsMock() as rsps:
        rsps.add(method=responses.POST,
                 url=mock_c8y.base_url + '/resource',
                 status=201,
                 json={'result': True})
        response = mock_c8y.post('/resource', json={'request': True})

        request_body = rsps.calls[0].request.body
        request_headers = rsps.calls[0].request.headers

        assert json.loads(request_body)['request']

        assert_auth_header(mock_c8y, request_headers)
        assert_accept_header(request_headers)
        assert_content_header(request_headers)
        assert_application_key_header(mock_c8y, request_headers)
        assert_processing_mode_header(mock_c8y, request_headers)

        assert response['result']


def test_post_explicits(mock_c8y: CumulocityRestApi):
    """Verify the basic functionality of the POST requests."""

    with responses.RequestsMock() as rsps:
        rsps.add(method=responses.POST,
                 url=mock_c8y.base_url + '/resource',
                 status=201,
                 json={'result': True})
        response = mock_c8y.post('/resource', accept='custom/accept',
                                 content_type='custom/content', json={'request': True})

        request_body = rsps.calls[0].request.body
        request_headers = rsps.calls[0].request.headers

        assert json.loads(request_body)['request']

        assert_auth_header(mock_c8y, request_headers)
        assert_accept_header(request_headers, 'custom/accept')
        assert_content_header(request_headers, 'custom/content')
        assert_application_key_header(mock_c8y, request_headers)
        assert_processing_mode_header(mock_c8y, request_headers)

        assert response['result']


@pytest.mark.online
def test_get_default(httpbin_basic: CumulocityRestApi):
    """Verify that the get function with default parameters works as expected."""
    c8y = httpbin_basic

    # (1) with implicit parameters given and all default
    response = c8y.get(resource='/anything/resource?p1=v1&p2=v2')

    # auth header must always be present
    assert response['headers']['Authorization']
    # by default we accept JSON
    assert response['headers']['Accept'] == 'application/json'
    # inline parameters recognized
    assert response['args']['p1']
    assert response['args']['p2']


@pytest.mark.online
def test_get_explicit(httpbin_basic: CumulocityRestApi):
    """Verify that the get function with explicit parameters works as expected."""
    c8y = httpbin_basic
    response = c8y.get(resource='/anything/resource', params={'p1': 'v1', 'p2': 3}, accept='something/custom')

    # auth header must always be present
    assert response['headers']['Authorization']
    # expecting our custom accept header
    assert response['headers']['Accept'] == 'something/custom'
    # explicit parameters recognized
    assert response['args']['p1']
    assert response['args']['p2']


def test_get_ordered_response():
    """Verify that the response JSON can be ordered on request."""
    c8y = CumulocityRestApi(base_url='url', tenant_id='t12345', username='user', password='pass')

    with patch('requests.Session.get') as get_mock:
        mock_response = requests.Response()
        mock_response.status_code = 200
        mock_response._content = b'{"list": [1, 2, 3, 4, 5], "x": "xxx", "m": "mmm", "c": "ccc"}'
        get_mock.return_value = mock_response
        response = c8y.get('any', ordered=True)
        elements = list(response.items())

        # first element is a list
        assert elements[0][0] == 'list'
        assert elements[0][1] == [1, 2, 3, 4, 5]
        # 2nd to 4th are some elements in order
        assert (elements[1][0], elements[2][0], elements[3][0]) == ('x', 'm', 'c')


def test_get_404():
    """Verify that a 404 results in a KeyError and a message naming the missing resource."""
    c8y = CumulocityRestApi(base_url='url', tenant_id='t12345', username='user', password='pass')

    with patch('requests.Session.get') as get_mock:
        mock_response = requests.Response()
        mock_response.status_code = 404
        get_mock.return_value = mock_response
        with pytest.raises(KeyError) as error:
            c8y.get('some/key')
        assert 'some/key' in str(error)

@pytest.mark.parametrize(
    'name, code, ex',
    [
        ('get', 401, UnauthorizedError),
        ('get', 403, AccessDeniedError),
        ('get_file', 401, UnauthorizedError),
        ('get_file', 403, AccessDeniedError),
        ('post', 401, UnauthorizedError),
        ('post', 403, AccessDeniedError),
        ('post_file', 401, UnauthorizedError),
        ('post_file', 403, AccessDeniedError),
        ('put', 401, UnauthorizedError),
        ('put', 403, AccessDeniedError),
        ('delete', 401, UnauthorizedError),
        ('delete', 403, AccessDeniedError),
    ]
)
def test_40x(name, code, ex: type[HttpError]):
    """Verify that various HTTP error codes are raised as expected."""

    c8y = CumulocityRestApi(base_url='url', tenant_id='t12345', username='user', password='pass')
    method = name.split('_')[0]
    function = getattr(c8y, name)

    with patch(f'requests.Session.{method}') as method_mock:
        mock_response = requests.Response()
        mock_response.status_code = code
        method_mock.return_value = mock_response
        with pytest.raises(ex) as error:
            function('/resource', {})
        assert error.value.method == method.upper()
        assert error.value.code == code
        assert error.value.url == 'url/resource'


def test_delete_defaults(mock_c8y: CumulocityRestApi):
    """Verify the basic functionality of the DELETE requests."""

    with responses.RequestsMock() as rsps:
        rsps.add(method=responses.DELETE,
                 url=mock_c8y.base_url + '/resource',
                 status=204)
        mock_c8y.delete('/resource')

        request_headers = rsps.calls[0].request.headers
        assert_auth_header(mock_c8y, request_headers)
        assert_application_key_header(mock_c8y, request_headers)
        assert_processing_mode_header(mock_c8y, request_headers)


def test_empty_response(mock_c8y: CumulocityRestApi):
    """Verify that an empty GET/POST/PUT responses doesn't break the code."""

    with responses.RequestsMock() as rsps:
        rsps.add(method=responses.GET,
                 url=mock_c8y.base_url + '/resource',
                 status=200)
        mock_c8y.get('/resource')

    with responses.RequestsMock() as rsps:
        rsps.add(method=responses.POST,
                 url=mock_c8y.base_url + '/resource',
                 status=201)
        mock_c8y.post('/resource', json={})

    with responses.RequestsMock() as rsps:
        rsps.add(method=responses.PUT,
                 url=mock_c8y.base_url + '/resource',
                 status=200)
        mock_c8y.put('/resource', json={})
