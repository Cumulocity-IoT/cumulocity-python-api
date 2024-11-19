# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import DeviceGroup

from util.testing_util import RandomNameGenerator


def test_CRUD(live_c8y: CumulocityApi, safe_executor):
    """Verify that object-oriented create, update, and delete works as expected."""

    name = RandomNameGenerator.random_name(2)

    root = DeviceGroup(live_c8y, root=True, name=f'Root-{name}', custom_fragment={'test': True})
    child1 = DeviceGroup(live_c8y, name=f'Child1-{name}', custom_fragment={'test': True})
    child2 = DeviceGroup(live_c8y, name=f'Child2-{name}', custom_fragment={'test': True})

    root = root.create()
    child1 = child1.create()
    child2 = child2.create()
    try:
        # x) assign groups
        root.assign_child_group(child1)
        root.assign_child_group(child2)

        # x) select all root groups
        # -> our root folder should bin in there
        assert f'Root-{name}' in [x.name for x in live_c8y.group_inventory.get_all()]

        # x) select by parent
        child_names = [x.name for x in live_c8y.group_inventory.get_all(parent=root.id)]
        assert len(child_names) == 2
        assert all(x.startswith('Child') for x in child_names)
        assert all(x.endswith(name) for x in child_names)

        # x) update
        child2['another_fragment'] = {'data': 12345}
        child2 = child2.update()
        # -> updated data set in db
        assert live_c8y.group_inventory.get(child2.id).another_fragment.data == 12345

        # x) unassigning child groups
        root.unassign_child_group(child1.id)
        root.unassign_child_group(child2)
        # -> all children unassigned
        assert not live_c8y.group_inventory.get_all(parent=root.id)

        # x re-assign for the remainder of the test
        live_c8y.group_inventory.assign_children(root.id, child1.id, child2.id)

        # x) delete a device group
        child2.delete()
        # -> child2 is gone
        with pytest.raises(KeyError):
            live_c8y.group_inventory.get(child2.id)

        # x) delete root and cascade
        root.delete_tree()
        # -> root and remaining child are gone
        with pytest.raises(KeyError):
            live_c8y.group_inventory.get(child1.id)
        with pytest.raises(KeyError):
            live_c8y.group_inventory.get(root.id)

    except BaseException as e:
        safe_executor(root.delete)
        safe_executor(child1.delete)
        safe_executor(child2.delete)
        raise e


def test_CRUD2(live_c8y, safe_executor):
    """Verify that create, update, and delete via the API works as expected."""

    name = RandomNameGenerator.random_name(2)

    root = DeviceGroup(live_c8y, root=True, name=f'Root-{name}', custom_fragment={'test': True})
    child1 = DeviceGroup(live_c8y, name=f'Child1-{name}', custom_fragment={'test': True})
    child2 = DeviceGroup(live_c8y, name=f'Child2-{name}', custom_fragment={'test': True})

    live_c8y.group_inventory.create(root, child1, child2)
    root = live_c8y.group_inventory.get_all(type=DeviceGroup.ROOT_TYPE, name=root.name)[0]
    child1 = live_c8y.group_inventory.get_all(type=DeviceGroup.CHILD_TYPE, name=child1.name)[0]
    child2 = live_c8y.group_inventory.get_all(type=DeviceGroup.CHILD_TYPE, name=child2.name)[0]

    try:
        # 1) assign children
        live_c8y.group_inventory.assign_children(root.id, child1.id, child2.id)
        # -> all child groups are nested now
        child_names = [x.name for x in live_c8y.group_inventory.get_all(parent=root.id, type=DeviceGroup.CHILD_TYPE)]
        assert len(child_names) == 2
        assert all(x.startswith('Child') for x in child_names)
        assert all(x.endswith(name) for x in child_names)

        # 2) unassign child 1
        live_c8y.group_inventory.unassign_children(root.id, child1.id)
        # -> only child2 is still assigned
        child_names = [x.name for x in live_c8y.group_inventory.get_all(parent=root.id)]
        assert [child2.name] == child_names

        # 3) re-assign child1 1
        live_c8y.group_inventory.assign_children(root.id, child1.id)
        # -> we have two children again
        assert len(live_c8y.group_inventory.get_all(parent=root.id)) == 2

        # 4) remove the groups
        live_c8y.group_inventory.delete_trees(root.id)

    except BaseException as e:
        safe_executor(live_c8y.group_inventory.delete(root.id, child1.id, child2.id))
        raise e


def test_select(live_c8y: CumulocityApi, safe_executor):
    """Verify that selecting with different filters works as expected."""

    name = RandomNameGenerator.random_name(2)

    root = DeviceGroup(live_c8y, root=True, name=f'Root-{name}', custom_fragment={'test': True})
    child1 = DeviceGroup(live_c8y, name=f'Child1-{name}', custom_fragment={'test': True})

    root = root.create()
    child1 = child1.create()
    root.assign_child_group(child1)

    try:

        # 1) select via name (query)
        #  by default, only root folders are selected
        ids = [x.id for x in live_c8y.group_inventory.select(name=f'Root-{name}')]
        # -> only the root group is returned
        assert ids == [root.id]
        assert live_c8y.group_inventory.get_count(name=f'Root-{name}') == 1

        # 2) select child folders via owner (no query)
        found_ids = [x.id for x in live_c8y.group_inventory.select(type='c8y_DeviceSubGroup', owner=live_c8y.username)]
        # -> only the child group can be found
        assert child1.id in found_ids
        assert root.id not in found_ids
        assert live_c8y.group_inventory.get_count(type='c8y_DeviceSubGroup', owner=live_c8y.username) == len(found_ids)

        # 3) select child by parent and owner (implicit query)
        ids = [x.id for x in live_c8y.group_inventory.select(parent=root.id, owner=live_c8y.username)]
        # -> only the child is returned
        assert ids == [child1.id]
        assert live_c8y.group_inventory.get_count(parent=root.id, owner=live_c8y.username) == 1

        root.delete_tree()

    except BaseException as ex:
        safe_executor(root.delete)
        safe_executor(child1.delete)
        raise ex


def test_trees(live_c8y: CumulocityApi, safe_executor):
    """Verify that creation and deletion of device group trees works as
    expected."""

    name = RandomNameGenerator.random_name(2)
    root = DeviceGroup(live_c8y, root=True, name=f'Root-{name}').create()
    child1 = root.create_child(name=f'Child1-{name}')
    child2 = root.create_child(name=f'Child2-{name}')
    child11 = child1.create_child(name=f'Child11-{name}')
    child12 = child1.create_child(name=f'Child12-{name}')
    child21 = child2.create_child(name=f'Child21-{name}')

    # -> child 1 and child 2 are the only children of root
    assert {child1.id, child2.id} == {x.id for x in live_c8y.group_inventory.get_all(parent=root.id)}

    # -> child 1.1 and child 1.2 are the children of child 1
    assert {child11.id, child12.id} == {x.id for x in live_c8y.group_inventory.get_all(parent=child1.id)}

    # remove child 1 (cascading)
    live_c8y.group_inventory.delete_trees(child1.id)
    # -> children are gone as well
    assert not live_c8y.group_inventory.get_all(parent=child1.id)

    # remove root (cascading)
    root.delete_tree()
    # -> all created groups are gone
    with pytest.raises(KeyError):
        live_c8y.group_inventory.get(child21.id)


@pytest.mark.skip  # cascade=false doesn't work
def test_non_cascade_delete(live_c8y: CumulocityApi, safe_executor):
    """Verify that non-cascading delete works as expected."""
    name = RandomNameGenerator.random_name(2)
    root = DeviceGroup(live_c8y, root=True, name=f'Root-{name}').create()
    child1 = root.create_child(name=f'Child1-{name}')
    child2 = root.create_child(name=f'Child2-{name}')

    child1.create_child(name=f'Child11-{name}')
    child1.create_child(name=f'Child12-{name}')
    child2.create_child(name=f'Child21-{name}')

    # remove root folder (not cascading)
    root.delete()
    # -> children are still there
    assert live_c8y.group_inventory.get(child1.id)
    assert live_c8y.group_inventory.get(child2.id)

    # cleanup
    child1.delete_tree()
    child2.delete_tree()
