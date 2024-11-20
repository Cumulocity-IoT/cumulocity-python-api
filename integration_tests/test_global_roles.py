# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

import random

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import GlobalRole, User

from util.testing_util import RandomNameGenerator


def test_CRUD(live_c8y: CumulocityApi):  # noqa (case)
    """Verify that basic CRUD functionality works."""

    rolename = RandomNameGenerator.random_name()

    role = GlobalRole(c8y=live_c8y, name=rolename, description=f'{rolename} description')

    created_role = role.create()
    try:
        # 1) assert correct creation
        assert created_role.id
        assert created_role.name == rolename
        assert rolename in created_role.description

        # 2) update updatable fields
        created_role.name = f'{rolename}_2'
        created_role.description = f'Updated {created_role.description}'
        updated_role = created_role.update()

        # 3) assert updates
        assert updated_role.name == created_role.name
        assert updated_role.description == created_role.description
    finally:
        created_role.delete()

    # 4) assert deletion
    with pytest.raises(KeyError) as e:
        live_c8y.global_roles.get(rolename)
        assert rolename in str(e)


def test_select(live_c8y: CumulocityApi):
    """Verify that selection works as expected."""
    # (1) get all defined global roles
    all_roles = live_c8y.global_roles.get_all()

    # (2) create a user and assign roles
    username = RandomNameGenerator.random_name(2)
    email = f'{username}@c8y.com'
    user = User(live_c8y, username=username, email=email, enabled=True).create()
    selected_roles = random.sample(all_roles, k=5)
    for role in selected_roles:
        user.assign_global_role(role.id)

    # (3) select by user
    for role in live_c8y.global_roles.get_all(username=username):
        assert role.id in [x.id for x in selected_roles]


def test_updating_users(live_c8y: CumulocityApi, factory):
    """Verify that users can be added/removed to/from a global role."""

    rolename = RandomNameGenerator.random_name()
    role: GlobalRole = factory(GlobalRole(c8y=live_c8y, name=rolename, description=f'{rolename} description'))

    # -> initially the current user should not have this global role
    assert role.id not in live_c8y.users.get(live_c8y.username).global_role_ids

    # 1) add the current user to this global role
    role.add_users(live_c8y.username)
    # -> user should now have this global role assigned
    assert role.id in live_c8y.users.get(live_c8y.username).global_role_ids

    # 2) remove the current user from this global role
    role.remove_users(live_c8y.username)
    # -> user should not have this global role anymore
    assert role.id not in live_c8y.users.get(live_c8y.username).global_role_ids


def test_updating_permissions(live_c8y: CumulocityApi, factory):
    """Verify that permissions can be added/removed to/from a global role."""

    rolename = RandomNameGenerator.random_name()
    role: GlobalRole = factory(GlobalRole(c8y=live_c8y, name=rolename, description=f'{rolename} description'))

    # -> initially there should be no permissions
    assert not role.permission_ids
    new_permissions = {'ROLE_EVENT_READ', 'ROLE_ALARM_READ'}

    # 1) add some permissions
    role.add_permissions(*new_permissions)
    # -> new permissions should be added to db object
    assert live_c8y.global_roles.get(role.id).permission_ids == new_permissions

    # 2) remove a permission
    removed_permission = new_permissions.pop()
    role.remove_permissions(removed_permission)
    # -> permission should be removed in db as well
    assert live_c8y.global_roles.get(role.id).permission_ids == new_permissions
