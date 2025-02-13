# Copyright (c) 2025 Cumulocity GmbH

import json
import os
import random

from c8y_api.model import InventoryRole, ReadPermission, WritePermission, Permission


def test_parsing():
    """Verify that parsing a InventoryRole from JSON works."""
    path = os.path.dirname(__file__) + '/inventoryrole.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        role_json = json.load(f)
    role = InventoryRole.from_json(role_json)

    assert role.id == role_json['id']
    assert role.name == role_json['name']
    assert role.description == role_json['description']

    permissions = {p.id: p for p in role.permissions}
    assert set(permissions.keys()) == {p['id'] for p in role_json['permissions']}

    for p in role_json['permissions']:
        pid = p['id']
        assert permissions[pid].type == p['type']
        assert permissions[pid].scope == p['scope']
        assert permissions[pid].level == p['permission']


def test_formatting():
    """Verify that formatting an InventoryRole as JSON works as expected."""
    role = InventoryRole(name='SomeRole', description='SomeDescription',
                         permissions=[ReadPermission(scope=Permission.Scope.ANY),
                                      WritePermission(scope=Permission.Scope.MEASUREMENT, type='c8y_Custom')])
    # hacking in permission ID:
    for p in role.permissions:
        p.id = random.randint(1, 999)

    full_json = role.to_json(only_updated=False)
    assert full_json['name'] == role.name
    assert full_json['description'] == role.description

    json_permissions = {p['id']: p for p in full_json['permissions']}
    for p in role.permissions:
        json_permission = json_permissions[p.id]
        assert json_permission['type'] == p.type
        assert json_permission['scope'] == p.scope
        assert json_permission['permission'] == p.level


def test_formatting_diff():
    """Verify that diff formatting an InventoryRole as JSON works as expected."""
    role = InventoryRole(name='SomeRole', description='SomeDescription',
                         permissions=[ReadPermission(scope=Permission.Scope.ANY),
                                      WritePermission(scope=Permission.Scope.MEASUREMENT, type='c8y_Custom')])
    # hacking in permission ID:
    for p in role.permissions:
        p.id = random.randint(1, 999)

    # writing an update and building diff json
    role.name = "NewName"
    diff_json = role.to_json(only_updated=True)
    # -> name is updated in JSON
    assert diff_json['name'] == role.name
    # -> description is not in the diff
    assert 'description' not in diff_json
    # -> all permissions are always there
    assert len(diff_json['permissions']) == len(role.permissions)
