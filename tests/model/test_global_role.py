# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=redefined-outer-name

from __future__ import annotations

import json
import os

import pytest

from c8y_api.model import GlobalRole


@pytest.fixture(scope='function')
def sample_role() -> GlobalRole:
    """Provide a sample global role, read from JSON file."""
    path = os.path.dirname(__file__) + '/global_role.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        role_json = json.load(f)

    return GlobalRole.from_json(role_json)


def test_parsing():
    """Verify that parsing a GlobalRole from JSON works."""
    path = os.path.dirname(__file__) + '/global_role.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        role_json = json.load(f)
    role = GlobalRole.from_json(role_json)

    assert role.id == str(role_json['id'])
    assert role.name == role_json['name']
    assert role.description == role_json['description']

    expected_permissions = {x['role']['id'] for x in role_json['roles']['references']}
    assert role.permission_ids == expected_permissions

    expected_applications = {x['id'] for x in role_json['applications']}
    assert role.application_ids == expected_applications


def test_formatting(sample_role: GlobalRole):
    """Verify that rendering a global role as JSON works as expected."""
    role_json = sample_role.to_json()
    assert 'id' not in role_json
    # we only expect
    expected_keys = {'name', 'description'}
    assert set(role_json.keys()) == expected_keys


def test_updating(sample_role: GlobalRole):
    """Verify that updating the global role properties are recorded properly."""

    # testing readonly fields
    sample_role.id = 'new id'
    sample_role.permission_ids = {'NEW_PERMISSION'}
    sample_role.application_ids = {'1', '2'}

    assert not sample_role.get_updates()
    assert sample_role.to_diff_json() == {}

    # testing updatable fields
    sample_role.name = 'new_name'
    sample_role.description = 'new description'

    expected_updates = {'name', 'description'}
    assert len(sample_role.get_updates()) == len(expected_updates)
    assert set(sample_role.to_diff_json().keys()) == expected_updates
