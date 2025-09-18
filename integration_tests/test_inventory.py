# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=redefined-outer-name

import random
import time
from typing import List

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Event, ManagedObject, Measurement, Count, Value, Device
from c8y_api.model.matcher import jsonpath

from util.testing_util import RandomNameGenerator
from tests.utils import get_ids


@pytest.fixture(scope='function')
def mutable_object(module_factory) -> ManagedObject:
    """Provide a single managed object ready to be changed during a test."""

    name = RandomNameGenerator.random_name(2)
    return module_factory(
        ManagedObject(name=name, type=name, **{name: {'key': 'value'}})
    )


def test_update(mutable_object: ManagedObject):
    """Verify that updating managed objects works as expected."""

    mutable_object.name = mutable_object.name + '_altered'
    mutable_object.type = mutable_object.type + '_altered'
    mutable_object['new_attribute'] = 'value'
    mutable_object['new_fragment'] = {'key': 'value'}
    updated_object = mutable_object.update()

    assert updated_object.name == mutable_object.name
    assert updated_object.type == mutable_object.type
    assert updated_object.new_attribute == 'value'
    assert updated_object.new_fragment.key == 'value'


@pytest.fixture(scope='module')
def similar_objects(module_factory) -> List[ManagedObject]:
    """Provide a list of similar ManagedObjects (different name, everything
    else identical).  These are not to be changed."""

    n = 5
    basename = RandomNameGenerator.random_name(2)
    typename = basename

    return [
        module_factory(
            ManagedObject(name=f'{basename}_{i}', type=typename, **{f'{typename}_fragment': {}})
        )
        for i in range(1, n + 1)
    ]


def test_get_all(live_c8y: CumulocityApi):
    """Verify that the get_all query works as expected."""
    # (1) get all devices
    devices = live_c8y.device_inventory.get_all(limit=1000)
    assert all('c8y_IsDevice' in d for d in devices)
    # (2) get all managed objects
    objects = live_c8y.inventory.get_all(limit=1000)
    # -> there should be both device and non-device objects
    device_objects = [o for o in objects if 'c8y_IsDevice' in o]
    assert len(objects) > len(device_objects)
    # (3) get all device groups
    groups = live_c8y.group_inventory.get_all(limit=1000)
    assert all('c8y_IsDeviceGroup' in g for g in groups)


@pytest.mark.parametrize('key, value_fun', [
    ('type', lambda mo: mo.type),
    ('name', lambda mo: mo.type + '*'),
    ('fragment', lambda mo: mo.type + '_fragment')
])
def test_get_by_something(live_c8y: CumulocityApi, similar_objects: List[ManagedObject], key, value_fun):
    """Verify that managed objects can be selected by common type."""
    kwargs = {key: value_fun(similar_objects[0])}
    selected_mos = live_c8y.inventory.get_all(**kwargs)
    assert get_ids(similar_objects) == get_ids(selected_mos)
    assert live_c8y.inventory.get_count(**kwargs) == len(similar_objects)


@pytest.mark.parametrize('query, value_fun', [
    ('type eq {}', lambda mo: mo.type),
    ('$filter=type eq {} $orderby=id', lambda mo: mo.type),
    ('$filter=name eq {}', lambda mo: mo.type + '*'),
    ('has({})', lambda mo: mo.type + '_fragment'),
])
def test_get_by_query(live_c8y: CumulocityApi, similar_objects: List[ManagedObject], query: str, value_fun):
    """Verify that the selection by query works as expected."""
    query = query.replace('{}', value_fun(similar_objects[0]))
    selected_mos = live_c8y.inventory.get_all(query=query)
    assert get_ids(similar_objects) == get_ids(selected_mos)
    assert live_c8y.inventory.get_count(query=query) == len(similar_objects)


def test_filtering(live_c8y: CumulocityApi, safe_create):
    """Verify that client side filtering works as expected."""
    objects = [
        safe_create(ManagedObject(
            live_c8y,
            type='c8y_TestObject',
            name=f'c8y_TestObject_{i}',
            array=random.choices(range(10), k=5)
        ))
        for i in range(10)
    ]

    # using filter parameter (JSONPath)
    filtered_1 = live_c8y.inventory.get_all(
        type='c8y_TestObject',
        fragment='array',
        filter=jsonpath('$.array[?(@ == 0)]'))
    # using Python means
    filtered_2 = list(filter(
        lambda mo: 'array' in mo and 0 in mo.array,
        live_c8y.inventory.select(type='c8y_TestObject', fragment='array')
    ))
    # -> no difference
    assert {x.name for x in filtered_1} == {x.name for x in filtered_2}

    for o in objects:
        o.delete()


def test_get_single_by_query(live_c8y: CumulocityApi, module_factory):
    """Verify that the get_by function works as expected."""
    basename = RandomNameGenerator.random_name(2)
    typename = basename

    # create a couple of objects with two types
    objects = [
        module_factory(ManagedObject(name=f'{basename}_1', type=f'{typename}_A')),
        module_factory(ManagedObject(name=f'{basename}_2', type=f'{typename}_A')),
        module_factory(ManagedObject(name=f'{basename}_3', type=f'{typename}_B')),
    ]

    # -> single matching query returns expected object
    assert live_c8y.inventory.get_by(type=f'{typename}_B').id == objects[2].id

    # -> not matching query returns expected object
    with pytest.raises(ValueError) as error:
        live_c8y.inventory.get_by(type=f'{typename}_C')
    assert "no matching object found" in str(error).lower()

    # -> not matching query returns expected object
    with pytest.raises(ValueError) as error:
        live_c8y.inventory.get_by(type=f'{typename}_A')
    assert "ambiguous" in str(error).lower()


def test_get_availability(live_c8y: CumulocityApi, session_device: Device):
    """Verify that the latest availability can be retrieved."""
    # set a required update interval
    session_device['c8y_RequiredAvailability'] = {'responseInterval': 10}
    session_device.update()
    # create an event to trigger update
    live_c8y.events.create(Event(type='c8y_TestEvent', time='now', source=session_device.id, text='Event!'))
    # verify availability information is defined
    # -> the information is updated asynchronously, hence this may be delayed
    availability = None
    for i in range(1, 8):
        time.sleep(pow(2, i))
        try:
            availability = live_c8y.inventory.get_latest_availability(session_device.id)
            assert availability.last_message_date
            break
        except KeyError:
            print("Availability not yet available (pun intended). Retrying ...")
    assert availability


def test_reload(live_c8y):
    """Verify that the reload function works as expected.

    We only need to test this for the ManagedObject class, implicitly verifying
    the _reload function. The correct instrumentation of this abstract function
    by other inventory objects is verified through a unit test.
    """
    name = RandomNameGenerator.random_name()
    obj0 = ManagedObject(live_c8y, name=f'Root-{name}', type=f'Root-{name}').create()

    # add a fragment
    live_c8y.inventory.apply_to({'c8y_AdditionalFragment': {'key': 'value'}}, obj0.id)
    obj1 = obj0.reload()
    # -> should be read from Cumulocity
    assert obj1.name == obj0.name
    assert obj1.creation_time == obj0.creation_time
    assert obj1.c8y_AdditionalFragment.key == 'value'

    # remove a fragment
    live_c8y.inventory.apply_to({'c8y_AdditionalFragment': None}, obj0.id)
    obj2 = obj0.reload()
    # -> should be removed when reloaded
    assert 'c8y_AdditionalFragment' not in obj2.fragments


@pytest.fixture(name='asset_hierarchy_root_id', scope='module')
def fix_asset_hierarchy_root_id(module_factory):
    """Provide a (read-only) sample asset hierarchy for corresponding tests.

    This fixture creates a root object with a child of each kind (asset,
    device, addition). Each of the children references to another 'addition'
    child to create a multi-level hierarchy.

    It is automatically cleaned up after testing.
    """
    name = RandomNameGenerator.random_name()
    obj = module_factory(ManagedObject(name=f'Root-{name}', type=f'Root-{name}'))

    addition = module_factory(ManagedObject(name=f'Addition-{name}', type=f'Addition-{name}'))
    asset = module_factory(ManagedObject(name=f'Asset-{name}', type=f'Asset-{name}'))
    device = module_factory(Device(name=f'Device-{name}', type=f'Device-{name}'))
    obj.add_child_addition(addition)
    obj.add_child_asset(asset)
    obj.add_child_device(device)

    sub_addition =  module_factory(ManagedObject(name=f'SubAddition-{name}', type=f'Addition-{name}'))
    addition.add_child_addition(sub_addition)
    asset.add_child_addition(sub_addition)
    device.add_child_addition(sub_addition)

    return obj.id


def test_references(live_c8y: CumulocityApi, asset_hierarchy_root_id):
    """Verify that parent references are handles as expected.

    This test uses the "asset_hierarchy" fixture which defines a root
    with children of each kind.
    """
    root_id = asset_hierarchy_root_id

    # (1) ignore children and parents
    result = live_c8y.inventory.get(root_id, with_children=False)
    assert not result.child_assets
    assert not result.child_devices
    assert not result.child_additions
    assert not result.parent_assets
    assert not result.parent_devices
    assert not result.parent_additions

    # (2) include children, with names
    result = live_c8y.inventory.get(root_id, with_children=True, skip_children_names=False)
    # -> the root object references one of each
    assert len(result.child_assets) == 1
    assert len(result.child_devices) == 1
    assert len(result.child_additions) == 1
    # -> including their names
    assert result.child_assets[0].name
    assert result.child_devices[0].name
    assert result.child_additions[0].name
    # -> but no parents
    assert not result.parent_assets
    assert not result.parent_devices
    assert not result.parent_additions

    # (3) include children, no names
    result = live_c8y.inventory.get(root_id, with_children=True, skip_children_names=True)
    # -> the root object references one of each
    assert len(result.child_assets) == 1
    assert len(result.child_devices) == 1
    assert len(result.child_additions) == 1
    # -> including their names
    assert not result.child_assets[0].name
    assert not result.child_devices[0].name
    assert not result.child_additions[0].name


@pytest.mark.parametrize('child_type', ['asset', 'device', 'addition'])
def test_parent_references(live_c8y: CumulocityApi, asset_hierarchy_root_id, child_type):
    """Verify that parent references are handles as expected.

    This test uses the "asset_hierarchy" fixture which defines a root
    with children of each kind. Each kind has another "addition" child.
    """
    root = live_c8y.inventory.get(asset_hierarchy_root_id, with_children=True)
    child = root.__dict__[f'child_{child_type}s'][0]

    # read child with references
    result = live_c8y.inventory.get(child.id,  with_children=True, with_parents=True)
    # parent (root) is linked by the child's type
    parents = result.__dict__[f'parent_{child_type}s']
    assert len(parents) == 1
    assert parents[0].id == root.id
    assert parents[0].name == root.name
    # each child as an 'addition' child
    assert len(result.child_additions) == 1

    # using get_all
    result2 = live_c8y.inventory.get_all(name=child.name, with_children=True, with_parents=True)
    assert len(result2) == 1
    parents2 = result2[0].__dict__[f'parent_{child_type}s']
    assert parents[0].id == parents2[0].id
    assert parents[0].name == parents2[0].name


def test_deletion(live_c8y: CumulocityApi, safe_create):
    """Verify that deletion works as expected.

    This test creates a managed object tree (root plus child asset, child device and child addition).
    Deleting the root object will not delete the children unless the 'cascade' option is used
    (using the delete_tree function).
    """
    name = RandomNameGenerator.random_name()
    obj = safe_create(ManagedObject(name=f'Root-{name}', type=f'Root-{name}'))
    addition = safe_create(ManagedObject(name=f'Addition-{name}', type=f'Addition-{name}'))
    asset = safe_create(ManagedObject(name=f'Asset-{name}', type=f'Asset-{name}'))
    device = safe_create(Device(name=f'Device-{name}', type=f'Device-{name}'))
    obj.add_child_addition(addition)
    obj.add_child_asset(asset)
    obj.add_child_device(device)

    obj = obj.reload()
    assert len(obj.child_additions) == 1
    assert obj.child_additions[0].id == addition.id
    assert len(obj.child_assets) == 1
    assert obj.child_assets[0].id == asset.id
    assert len(obj.child_devices) == 1
    assert obj.child_devices[0].id == device.id

    # delete the root managed object
    obj.delete()
    # -> everything else is still around
    addition.reload()
    asset.reload()
    device.reload()

    # assign to a new root
    obj = safe_create(ManagedObject(name=f'Root-{name}', type=f'Root-{name}'))
    obj.add_child_addition(addition)
    obj.add_child_asset(asset)
    obj.add_child_device(device)
    # delete tree
    obj.delete_tree()
    # -> everything else is gone as well
    with pytest.raises(KeyError):
        addition.reload()
    with pytest.raises(KeyError):
        asset.reload()
    with pytest.raises(KeyError):
        device.reload()


def test_device_deletion(live_c8y: CumulocityApi, safe_create):
    """Verify that device deletion works as expected.

    This test creates a device tree (root plus child asset, child device and child addition).
    Deleting the root device will not delete the children unless the 'cascade' option is used
    (using the delete_tree function).
    """
    name = RandomNameGenerator.random_name()
    obj = safe_create(Device(name=f'Root-{name}', type=f'Root-{name}'))
    addition = safe_create(ManagedObject(name=f'Addition-{name}', type=f'Addition-{name}'))
    asset = safe_create(ManagedObject(name=f'Asset-{name}', type=f'Asset-{name}'))
    device = safe_create(Device(name=f'Device-{name}', type=f'Device-{name}'))
    obj.add_child_addition(addition)
    obj.add_child_asset(asset)
    obj.add_child_device(device)

    obj = obj.reload()
    assert len(obj.child_additions) == 1
    assert obj.child_additions[0].id == addition.id
    assert len(obj.child_assets) == 1
    assert obj.child_assets[0].id == asset.id
    assert len(obj.child_devices) == 1
    assert obj.child_devices[0].id == device.id

    # delete the root managed object
    obj.delete()
    # -> everything else is still around
    addition.reload()
    asset.reload()
    device.reload()

    # assign to a new root
    obj = safe_create(Device(name=f'Root-{name}', type=f'Root-{name}'))
    obj.add_child_addition(addition)
    obj.add_child_asset(asset)
    obj.add_child_device(device)
    # delete tree
    obj.delete_tree()
    # -> everything else is gone as well
    with pytest.raises(KeyError):
        addition.reload()
    with pytest.raises(KeyError):
        asset.reload()
    with pytest.raises(KeyError):
        device.reload()


@pytest.fixture
def object_with_measurements(live_c8y: CumulocityApi, mutable_object: ManagedObject) -> ManagedObject:
    """Provide a managed object with predefined measurements."""
    ms = [Measurement(live_c8y, type='c8y_TestMeasurementType', source=mutable_object.id, time='now',
                      c8y_Counter = {'N': Count(i)},
                      c8y_Integers = {'V1': Value(i, ''),
                                      'V2' : Value(i*i, '')}) for i in range(5)]
    live_c8y.measurements.create(*ms)
    return mutable_object


def test_get_supported_measurements(live_c8y: CumulocityApi, object_with_measurements: ManagedObject):
    """Verify that the supported measurements can be retrieved."""
    result = live_c8y.inventory.get_supported_measurements(object_with_measurements.id)
    assert set(result) == {'c8y_Counter', 'c8y_Integers'}


def test_get_supported_measurements_2(live_c8y: CumulocityApi, object_with_measurements: ManagedObject):
    """Verify that the supported measurements can be retrieved."""
    result = object_with_measurements.get_supported_measurements()
    assert set(result) == {'c8y_Counter', 'c8y_Integers'}


def test_get_supported_series(live_c8y: CumulocityApi, object_with_measurements: ManagedObject):
    """Verify that the supported measurement series can be retrieved."""
    result = live_c8y.inventory.get_supported_series(object_with_measurements.id)
    assert set(result) == {'c8y_Counter.N', 'c8y_Integers.V1', 'c8y_Integers.V2'}


def test_get_supported_series_2(live_c8y: CumulocityApi, object_with_measurements: ManagedObject):
    """Verify that the supported measurement series can be retrieved."""
    result = object_with_measurements.get_supported_series()
    assert set(result) == {'c8y_Counter.N', 'c8y_Integers.V1', 'c8y_Integers.V2'}
