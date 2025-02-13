# Copyright (c) 2025 Cumulocity GmbH

import json
import os

from typing import List
from unittest.mock import Mock

import pytest

from c8y_api import CumulocityRestApi
from c8y_api.model.tenants import Tenant

from tests.utils import isolate_last_call_arg


def fix_sample_jsons() -> List[dict]:
    """Read sample jsons from file. This is not a pytest fixture."""
    path = os.path.dirname(__file__) + '/tenants.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        tenants = json.load(f)
        return tenants['tenants']


@pytest.mark.parametrize('sample_json', fix_sample_jsons())
def test_parsing(sample_json):
    """Verify that parsing a Tenant from JSON works as expected."""
    tenant = Tenant.from_json(sample_json)

    assert tenant.id == sample_json['id']
    assert tenant.parent == sample_json['parent']
    assert tenant.creation_time == sample_json['creationTime']
    assert tenant.status == sample_json['status']
    assert tenant.domain == sample_json['domain']
    assert tenant.admin_name == sample_json['adminName']

    if 'adminEmail' in sample_json:
        assert tenant.admin_email == sample_json['adminEmail']
    if 'company' in sample_json:
        assert tenant.company == sample_json['company']
    if 'contactName' in sample_json:
        assert tenant.contact_name == sample_json['contactName']
    if 'contactPhone' in sample_json:
        assert tenant.contact_phone == sample_json['contactPhone']

    if 'applications' in sample_json:
        for a in zip(tenant.applications, sample_json['applications']['references']):
            assert a[0].id == a[1]['application']['id']
            assert a[0].owner == a[1]['application']['owner']['tenant']['id']

    if 'ownedApplications' in sample_json:
        for a in zip(tenant.owned_applications, sample_json['ownedApplications']['references']):
            assert a[0].id == a[1]['application']['id']
            assert a[0].owner == a[1]['application']['owner']['tenant']['id']


def test_formatting():
    """Verify that JSON formatting works as expected."""

    tenant = Tenant(
        domain='domain.com',
        admin_name='admin_name@email.com',
        admin_email='admin_email@email.com',
        admin_pass='admin_pass',
        company='company name',
        contact_name='contact name',
        contact_phone='contact phone'
    )

    tenant_json = tenant.to_full_json()

    # some core information can only be created by Cumulocity
    assert 'id' not in tenant_json
    assert 'parent' not in tenant_json
    assert 'status' not in tenant_json
    # business data should be mapped completely
    assert tenant_json['domain'] == tenant.domain
    assert tenant_json['adminName'] == tenant.admin_name
    assert tenant_json['adminEmail'] == tenant.admin_email
    assert tenant_json['company'] == tenant.company
    assert tenant_json['contactName'] == tenant.contact_name
    assert tenant_json['contactPhone'] == tenant.contact_phone


def test_create():
    """Verify that the object creation works as expected (JSON & URLs)."""
    mock_result = {
        'id': 't1234',
        'domain': 'domain'
    }

    c8y: CumulocityRestApi = Mock()
    c8y.post = Mock(return_value=mock_result)

    tenant = Tenant(c8y=c8y)
    tenant.to_json = Mock(return_value={'expected': True})
    updated_tenant = tenant.create()

    # 1) to_json should have been called
    assert tenant.to_json.call_count == 1
    # 2) the given resource path should be correct
    resource = isolate_last_call_arg(c8y.post, 'resource', 0)
    assert resource == '/tenant/tenants'
    # 3) the given payload should match what to_json returned
    payload = isolate_last_call_arg(c8y.post, 'json', 1)
    assert set(payload.keys()) == {'expected'}
    # 4) the return should be parsed properly
    assert updated_tenant.id == mock_result['id']
    assert updated_tenant.domain == mock_result['domain']


def test_update():
    """Verify that the object update works as expected (JSON & URLs)."""
    mock_result = {
        'id': 't1234',
        'domain': 'domain'
    }

    c8y: CumulocityRestApi = Mock()
    c8y.put = Mock(return_value=mock_result)

    tenant = Tenant(c8y=c8y)
    tenant.id = 'tenant-id'
    tenant.to_json = Mock(return_value={'expected': True})
    updated_tenant = tenant.update()

    # 1) to_json should have been called
    assert tenant.to_json.call_count == 1
    # 2) the given resource path should be correct
    resource = isolate_last_call_arg(c8y.put, 'resource', 0)
    assert resource == f'/tenant/tenants/{tenant.id}'
    # 3) the given payload should match what to_json returned
    payload = isolate_last_call_arg(c8y.put, 'json', 1)
    assert set(payload.keys()) == {'expected'}
    # 4) the return should be parsed properly
    assert updated_tenant.id == mock_result['id']
    assert updated_tenant.domain == mock_result['domain']


def test_delete():
    """Verify that the object deletion works as expected."""
    c8y: CumulocityRestApi = Mock()
    c8y.delete = Mock()

    tenant = Tenant(c8y=c8y)
    tenant.id = 'tenant-id'

    tenant.delete()

    assert c8y.delete.call_count == 1
    # 2) the given resource path should be correct
    resource = isolate_last_call_arg(c8y.delete, 'resource', 0)
    assert resource == f'/tenant/tenants/{tenant.id}'
