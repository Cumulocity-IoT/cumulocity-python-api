# Copyright (c) 2025 Cumulocity GmbH

import uuid

import pytest
import responses
from requests.auth import HTTPBasicAuth

from c8y_api import CumulocityDeviceRegistry


def test_auth():
    """Verify that the basic auth information is piped through correctly."""
    base_url = 'https://baseurl.com:8989'
    tenant_id = 't12345'
    username = 'someuser'
    password = 'somepass'

    c8y_registry = CumulocityDeviceRegistry(base_url=base_url, tenant_id=tenant_id,
                                            username=username, password=password)
    assert c8y_registry.username == username
    assert c8y_registry.tenant_id == tenant_id
    assert isinstance(c8y_registry.auth, HTTPBasicAuth)
    assert c8y_registry.auth.username == f'{tenant_id}/{username}'
    assert c8y_registry.auth.password == password


@pytest.fixture(name='device_registry')
def fix_device_registry():
    """Build a sample device registry API instance."""
    base_url = 'https://baseurl.com:8989'
    tenant_id = 't12345'
    username = 'someuser'
    password = 'somepass'

    return CumulocityDeviceRegistry(base_url=base_url, tenant_id=tenant_id, username=username, password=password)


def test_awaiting_connection(device_registry):
    """Verify that a GET request uses the right credentials."""

    device_serial = str(uuid.uuid1())

    body_matchers = [responses.json_params_matcher({'id': device_serial})]
    expected_url = device_registry.base_url + '/devicecontrol/deviceCredentials'
    response = {
      'id': device_serial,
      'self': 'not relevant',
      'tenantId':  device_registry.tenant_id,
      'username': 'device_' + device_serial,
      'password': 'password12345'
    }

    with responses.RequestsMock() as rsps:
        # -> 1st request should be denied
        rsps.add(method='POST', url=expected_url, status=404, match=body_matchers)
        # -> 2nd request will be the ok
        rsps.add(method='POST', url=expected_url, status=201, match=body_matchers,json=response)

        credentials = device_registry.await_credentials(device_serial)
        assert credentials.tenant_id == device_registry.tenant_id
        assert credentials.username == response['username']
        assert credentials.password == response['password']
