# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import User, InventoryRole, Permission, ReadPermission, WritePermission, AnyPermission

from util.testing_util import RandomNameGenerator


def test_CRUD(live_c8y):
    """Verify that object-oriented create, update and delete works."""

    permissions = [ReadPermission(scope=Permission.Scope.ANY),
                   WritePermission(scope=Permission.Scope.MEASUREMENT, type='c8y_Custom'),
                   AnyPermission(scope=Permission.Scope.ALARM, type='*')]
    role = InventoryRole(name=RandomNameGenerator.random_name(2), description='SomeDescription',
                         permissions=permissions)

    # 1) create role
    role.c8y = live_c8y
    role = role.create()
    # -> ids are set
    assert role.id
    assert all(p.id for p in role.permissions)

    # 2) update the role
    role.description = 'new description'
    del role.permissions[0]
    updated_role = role.update()
    # -> updated role has all the changed fields
    assert updated_role.id == role.id
    assert updated_role.description == role.description
    # -> the ID of the permissions should persist
    assert {p.id for p in updated_role.permissions} == {p.id for p in role.permissions}

    # 3) delete the role
    role.delete()
    # -> verify that the role is gone
    # (this used to throw a SyntaxError, KeyError since 10.20)
    with pytest.raises(KeyError):
        live_c8y.inventory_roles.get(role.id)


def test_CRUD2(live_c8y):
    """Verify that API-based create, update and delete works."""

    permissions = [ReadPermission(scope=Permission.Scope.ANY),
                   WritePermission(scope=Permission.Scope.MEASUREMENT, type='c8y_Custom'),
                   AnyPermission(scope=Permission.Scope.ALARM, type='*')]
    role = InventoryRole(name=RandomNameGenerator.random_name(2), description='SomeDescription',
                         permissions=permissions)

    # 1) create role
    live_c8y.inventory_roles.create(role)

    # 2) get all roles
    all_roles = live_c8y.inventory_roles.get_all()
    # -> created role can be found
    created_role = next(filter(lambda r: r.name == role.name, all_roles))

    # 3) can be updated
    created_role.description = 'new description'
    live_c8y.inventory_roles.update(created_role)

    # 4) directly grab from DB
    updated_role = live_c8y.inventory_roles.get(created_role.id)
    # -> it was updated
    assert updated_role.description == created_role.description

    # 5) delete the role
    live_c8y.inventory_roles.delete(created_role.id)
    # -> verify that the role is gone
    # (this used to throw a SyntaxError, KeyError since 10.20)
    with pytest.raises(KeyError):
        live_c8y.inventory_roles.get(created_role.id)


def test_select_inventory_roles(live_c8y: CumulocityApi):
    """Verify that selection works as expected."""
    # (1) get all defined inventory roles
    assert live_c8y.inventory_roles.get_all()


def test_assignments(live_c8y, sample_device, factory):
    """Verify that inventory roles can be assigned, retrieved and unassigned."""
    email = 'user_' + RandomNameGenerator.random_name(2) + '@test.com'
    role1_name = 'role_' + RandomNameGenerator.random_name(2)
    role2_name = 'role_' + RandomNameGenerator.random_name(2)

    # create a user
    user = User(username=email, email=email)
    user = factory(user)

    # create inventory roles
    role1 = InventoryRole(name=role1_name, permissions=[
        ReadPermission(scope=Permission.Scope.ALARM),
        WritePermission(scope=Permission.Scope.AUDIT)])
    role1 = factory(role1)
    role2 = InventoryRole(name=role2_name, permissions=[
        ReadPermission(scope=Permission.Scope.ANY),
        WritePermission(scope=Permission.Scope.MEASUREMENT)])
    role2 = factory(role2)

    # assign inventory roles
    user.assign_inventory_roles(sample_device.id, role1, role2)

    # verify that roles are assigned
    assigned_roles = user.retrieve_inventory_role_assignments()
    assert {role1_name, role2_name} == {x.name for x in assigned_roles[0].roles}

    # delete the assignment
    user.unassign_inventory_roles(assigned_roles[0].id)

    # verify that the assignment is gone
    assert not user.retrieve_inventory_role_assignments()
