# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=redefined-outer-name

from __future__ import annotations

import json
import os
from unittest.mock import Mock

import pytest

from c8y_api import CumulocityApi
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


def test_client_side_filtering():
    """Verify that client side filtering works as expected when selecting by username.

    The GlobalRoles API has a special case for selecting groups/global roles by username
    which is covered by this test.

    See also `test_filtering.py` for generic client side filtering tests.
    """

    # create mock CumulocityApi instance
    c8y = CumulocityApi(base_url='some.host.com', tenant_id='t123', username='user', password='pass')

    # prepare mock data and corresponding matchers
    # the get function is invoked until there are no results (empty list), the results
    # are stored in an array by object name
    get_data = [
        {'references': x, 'statistics': {'totalPages': 1}} for x in
        [
            # need to prepare data for all kind of formats ...
            [{'group': {'id': str(x)}} for x in [1, 2, 3]],
            []
        ]
    ]
    include_results = [True, False, True]
    exclude_results = [True, False]

    c8y.get = Mock(side_effect=get_data)
    include_matcher = Mock(safe_matches=Mock(side_effect=include_results))
    exclude_matcher = Mock(safe_matches=Mock(side_effect=exclude_results))

    # run get_all/select
    result = c8y.global_roles.get_all(username='username', include=include_matcher, exclude=exclude_matcher)

    # -> result should only contain filtered documents
    #    1,2,3 -> 1,3 -> 3
    assert ['3'] == [str(x.id) for x in result]
    # -> include matcher should have been called for each document
    assert include_matcher.safe_matches.call_count == len(include_results)
    # -> exclude matcher should have been called for each included
    assert exclude_matcher.safe_matches.call_count == len(exclude_results)
