# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=redefined-outer-name

import time
from typing import List

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Event, ManagedObject, Measurement, Count, Value, Device

from util.testing_util import RandomNameGenerator
from tests.utils import get_ids


@pytest.fixture(scope='session')
def object_factory(logger, live_c8y: CumulocityApi):
    """Provides a generic factory function which creates given ManagedObject
    instances within the database and cleans up afterwards.

    This fixture is supposed to be used by other fixtures.
    """

    created_objs = []

    def factory_fun(*objs: ManagedObject):
        logger.info(f"Creating {len(objs)} ManagedObject instances in live Cumulocity instance ...")
        new_objects = []
        for obj in objs:  # noqa
            obj.c8y = live_c8y
            created_obj = obj.create()
            new_objects.append(created_obj)
            logger.info(f'Created ManagedObject: #{created_obj.id}, name: {obj.name}, type: {obj.type}')
        created_objs.extend(new_objects)
        return new_objects

    yield factory_fun

    logger.info("Removing previously created ManagedObject instances ...")
    for obj in created_objs:
        obj.delete()
        logger.info(f"Deleted ManagedObject: #{obj.id}")


@pytest.fixture(scope='function')
def mutable_object(object_factory) -> ManagedObject:
    """Provide a single managed object ready to be changed during a test."""

    name = RandomNameGenerator.random_name(2)
    typename = name

    mo = ManagedObject(name=name, type=typename, **{name: {'key': 'value'}})
    return object_factory(mo)[0]


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


@pytest.fixture(scope='session')
def similar_objects(object_factory) -> List[ManagedObject]:
    """Provide a list of similar ManagedObjects (different name, everything
    else identical).  These are not to be changed."""

    n = 5
    basename = RandomNameGenerator.random_name(2)
    typename = basename

    mos = [ManagedObject(name=f'{basename}_{i}', type=typename, **{f'{typename}_fragment': {}}) for i in range(1, n+1)]
    return object_factory(*mos)


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


def test_get_availability(live_c8y: CumulocityApi, sample_device: Device):
    """Verify that the latest availability can be retrieved."""
    # set a required update interval
    sample_device['c8y_RequiredAvailability'] = {'responseInterval': 10}
    sample_device.update()
    # create an event to trigger update
    live_c8y.events.create(Event(type='c8y_TestEvent', time='now', source=sample_device.id, text='Event!'))
    # verify availability information is defined
    # -> the information is updated asynchronously, hence this may be delayed
    availability = None
    for i in range(1, 8):
        time.sleep(pow(2, i))
        try:
            availability = live_c8y.inventory.get_latest_availability(sample_device.id)
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


def test_deletion(live_c8y: CumulocityApi, register_object):
    """Verify that deletion works as expected.

    This test creates a managed object tree (root plus child asset, child device and child addition).
    Deleting the root object will not delete the children unless the 'cascade' option is used
    (using the delete_tree function).
    """
    name = RandomNameGenerator.random_name()
    obj = register_object(ManagedObject(live_c8y, name=f'Root-{name}', type=f'Root-{name}').create())
    addition = register_object(ManagedObject(live_c8y, name=f'Addition-{name}', type=f'Addition-{name}').create())
    asset = register_object(ManagedObject(live_c8y, name=f'Asset-{name}', type=f'Asset-{name}').create())
    device = register_object(Device(live_c8y, name=f'Device-{name}', type=f'Device-{name}').create())
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
    obj = register_object(ManagedObject(live_c8y, name=f'Root-{name}', type=f'Root-{name}').create())
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


def test_device_deletion(live_c8y: CumulocityApi, register_object):
    """Verify that device deletion works as expected.

    This test creates a device tree (root plus child asset, child device and child addition).
    Deleting the root device will not delete the children unless the 'cascade' option is used
    (using the delete_tree function).
    """
    name = RandomNameGenerator.random_name()
    obj = register_object(Device(live_c8y, name=f'Root-{name}', type=f'Root-{name}').create())
    addition = register_object(ManagedObject(live_c8y, name=f'Addition-{name}', type=f'Addition-{name}').create())
    asset = register_object(ManagedObject(live_c8y, name=f'Asset-{name}', type=f'Asset-{name}').create())
    device = register_object(Device(live_c8y, name=f'Device-{name}', type=f'Device-{name}').create())
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
    obj = register_object(Device(live_c8y, name=f'Root-{name}', type=f'Root-{name}').create())
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
