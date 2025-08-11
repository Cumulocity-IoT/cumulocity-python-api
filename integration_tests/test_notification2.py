# Copyright (c) 2025 Cumulocity GmbH

import asyncio
import queue
import threading
import time

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Device, ManagedObject, Subscription, Measurement, Value, Event, Alarm, Operation
from c8y_tk.notification2 import AsyncListener, Listener
from tests.utils import assert_in_any

from util.testing_util import RandomNameGenerator


def test_subscription_deletion(live_c8y, safe_create):
    """Verify that a subscription is removed with the corresponding managed object."""
    mo_name = RandomNameGenerator.random_name(3)
    mo = safe_create(ManagedObject(name=f'{mo_name}1', type=f'test_{mo_name}'))

    Subscription(live_c8y, name=f'{mo_name.replace("_", "")}Subscription',
                 context=Subscription.Context.MANAGED_OBJECT, source_id=mo.id).create()

    assert live_c8y.notification2_subscriptions.get_count(source=mo.id) == 1
    mo.delete()
    assert live_c8y.notification2_subscriptions.get_count(source=mo.id) == 0


@pytest.fixture(name='object_tree_builder')
def fix_object_tree_builder(live_c8y: CumulocityApi, safe_create):
    """Provide a builder function which creates a root object with 3 children (asset, device, and addition)."""

    @pytest.mark.usefixtures('safe_create')
    def build():
        mo_name = RandomNameGenerator.random_name(3)
        type_name = f'test_{mo_name}'
        mo = safe_create(ManagedObject(live_c8y, name=mo_name, type=type_name))
        child_asset = safe_create(ManagedObject(live_c8y, name=f'{mo_name}_child_asset', type=type_name))
        child_device = safe_create(Device(live_c8y, name=f'{mo_name}_child_device', type=type_name))
        child_addition = safe_create(ManagedObject(live_c8y, name=f'{mo_name}_child_addition', type=type_name))
        mo.add_child_asset(child_asset)
        mo.add_child_device(child_device)
        mo.add_child_addition(child_addition)
        return mo.reload()

    return build


@pytest.mark.parametrize("api_filters, expected", [
    ('*', 'M,E,EwC,A,AwC,MO'),
    ('M', 'M'),
    ('E', 'E'),
    ('EwC', 'E,EwC'),
    ('A', 'A'),
    ('AwC', 'A,AwC'),
    ('MO', 'MO'),
], ids=[
    "*",
    'measurements',
    'events',
    'events+children',
    'alarms',
    'alarms+children',
    'managedObjects',
])
def test_api_filters(live_c8y: CumulocityApi, sample_object, api_filters, expected):
    """Verify that API filters work as expected.

    This test creates a subscription with selected API filters and performs
    a couple of corresponding changes. It then matches the received
    notifications against expectations.
    """
    # TODO: Add Operation
    apis = {
        '*': '*',
        'M': 'measurements',
        'E': 'events',
        'A': 'alarms',
        'EwC': 'eventsWithChildren',
        'AwC': 'alarmsWithChildren',
        'O': 'operations',
        'MO': 'managedobjects',
    }
    expected = [apis[x] for x in expected.split(',')]
    api_filters = [apis[x] for x in api_filters.split(',')]

    mo = sample_object
    mo['c8y_IsDevice'] = {}
    mo.update()
    sub = Subscription(
        live_c8y,
        name=f'{mo.name.replace("_", "")}Subscription',
        context=Subscription.Context.MANAGED_OBJECT,
        api_filter=api_filters,
        source_id=mo.id,
    ).create()

    notifications = queue.Queue()
    def receive_notification(m:Listener.Message):
        notifications.put(m)
        m.ack()

    # (1) Create listener and start listening
    listener = Listener(live_c8y, subscription_name=sub.name)
    listener_thread = threading.Thread(target=listener.listen, args=[receive_notification])
    listener_thread.start()
    try:
        time.sleep(3)  # ensure creation

        # (2) apply updates
        m_id = Measurement(live_c8y, source=mo.id, type="c8y_TestMeasurement", metric=Value(1, '')).create()
        e_id = Event(live_c8y, source=mo.id, type="c8y_TestEvent", time='now', text='text').create()
        a_id = Alarm(live_c8y, source=mo.id, type="c8y_TestAlarm", time='now', text='text', severity=Alarm.Severity.WARNING).create()
        # o_id = Operation(live_c8y, device_id=mo.id, c8y_Operation={}).create()
        mo.apply({'some_tag': {}})

        time.sleep(1)
        ns = list(notifications.queue)
        # collect message types from source URL
        types = {n.source.split('/')[2] for n in ns}
        for e in expected:
            assert_in_any(e, *types)

    finally:
        # (99) cleanup
        listener.close()
        listener_thread.join()


def test_object_update_and_deletion(live_c8y: CumulocityApi, sample_object):
    """Verify that we can subscribe to managed object changes and they are received.

    This test creates a simple managed object and a corresponding subscription; the subscription
    limits the 'fragments to copy'. We then apply 2 changes, both should be received but only the
    expected fragment should be part of the notification body.

    Finally, the deletion of the object should also be received.
    """

    mo = sample_object
    sub = Subscription(live_c8y, name=f'{mo.name.replace("_", "")}Subscription',
                       context=Subscription.Context.MANAGED_OBJECT,
                       fragments=['test_AwaitedFragment'],
                       source_id=mo.id,).create()

    notifications = queue.Queue()
    def receive_notification(m:Listener.Message):
        notifications.put(m)
        m.ack()

    # (1) Create listener and start listening
    listener = Listener(live_c8y, subscription_name=sub.name)
    listener_thread = threading.Thread(target=listener.listen, args=[receive_notification])
    listener_thread.start()
    time.sleep(5)  # ensure creation

    # (2) apply first change, expected fragment
    mo.apply({'test_AwaitedFragment': {'num': 42}})
    # -> notification should appear
    m = notifications.get(timeout=5)
    assert notifications.empty()
    assert mo.id in m.source
    assert m.action == "UPDATE"
    # -> basic data AND expected fragment in payload
    assert m.json['id'] == mo.id
    assert m.json['name'] == mo.name
    assert m.json['type'] == mo.type
    assert m.json['test_AwaitedFragment']['num'] == 42

    # (3) Apply 2nd change, different fragment
    mo.apply({'test_DifferentFragment': {'num': 42}})
    # -> notification should appear
    m = notifications.get(timeout=5)
    assert notifications.empty()
    assert mo.id in m.source
    assert m.action == "UPDATE"
    # -> basic data in payload
    assert m.json['id'] == mo.id
    assert m.json['name'] == mo.name
    assert m.json['type'] == mo.type
    # -> other fragment not in payload
    assert 'test_AwaitedFragment' in m.json
    assert 'test_DifferentFragment' not in m.json

    # (4) delete object tree
    mo.delete_tree()
    # -> notification should appear
    m = notifications.get(timeout=5)
    assert notifications.empty()
    assert mo.id in m.source
    assert m.action == "DELETE"

    # (99) cleanup
    listener.close()
    listener_thread.join()


@pytest.mark.asyncio(loop_scope='function')
async def test_asyncio_object_update_and_deletion(logger, live_c8y: CumulocityApi, safe_create):
    """Verify that we can subscribe to managed object changes and they are received.

    This test creates a simple managed object and a corresponding subscription; the subscription
    limits the 'fragments to copy'. We then apply 2 changes, both should be received but only the
    expected fragment should be part of the notification body.

    Finally, the deletion of the object should also be received.
    """

    mo_name = RandomNameGenerator.random_name(3)
    mo = safe_create(ManagedObject(live_c8y, name=f'{mo_name}1', type=f'test_{mo_name}'))
    sub = Subscription(live_c8y, name=f'{mo.name.replace("_", "")}Subscription',
                       context=Subscription.Context.MANAGED_OBJECT,
                       fragments=['test_AwaitedFragment'],
                       source_id=mo.id,).create()

    notifications = asyncio.Queue()
    async def receive_notification(m:AsyncListener.Message):
        await notifications.put(m)
        await m.ack()

    listener = AsyncListener(live_c8y, subscription_name=sub.name)
    listener_task = asyncio.create_task(listener.listen(receive_notification))
    await asyncio.sleep(5)  # ensure creation

    # (1) apply first change, expected fragment
    mo.apply({'test_AwaitedFragment': {'num': 42}})
    # -> notification should appear
    m = await notifications.get()
    assert notifications.empty()
    assert mo.id in m.source
    assert m.action == "UPDATE"
    # -> basic data AND expected fragment in payload
    assert m.json['id'] == mo.id
    assert m.json['name'] == mo.name
    assert m.json['type'] == mo.type
    assert m.json['test_AwaitedFragment']['num'] == 42

    # (2) Apply 2nd change, different fragment
    mo.apply({'test_DifferentFragment': {'num': 42}})
    # -> notification should appear
    m = await notifications.get()
    assert notifications.empty()
    assert mo.id in m.source
    assert m.action == "UPDATE"
    # -> basic data in payload
    assert m.json['id'] == mo.id
    assert m.json['name'] == mo.name
    assert m.json['type'] == mo.type
    # -> other fragment not in payload
    assert 'test_AwaitedFragment' in m.json
    assert 'test_DifferentFragment' not in m.json

    # (3) delete object tree
    mo.delete_tree()
    # -> notification should appear
    m = await notifications.get()
    assert notifications.empty()
    assert mo.id in m.source
    assert m.action == "DELETE"

    # (99) cleanup
    await listener.close()
    await listener_task


def test_child_updates(live_c8y: CumulocityApi, object_tree_builder):
    """Verify that updates to child objects are ignored."""
    root = object_tree_builder()

    root_subscription = Subscription(
        live_c8y,
        name=f'{root.name.replace("_", "")}Subscription',
        context=Subscription.Context.MANAGED_OBJECT, source_id=root.id
    ).create()

    # prepare listener
    notified = threading.Event()
    def receive_notification(m:Listener.Message):
        notified.set()
        m.ack()

    listener = Listener(live_c8y, subscription_name=root_subscription.name)
    listener_thread = threading.Thread(target=listener.listen, args=[receive_notification])
    listener_thread.start()
    time.sleep(5)  # ensure creation

    # update all child objects
    live_c8y.inventory.apply_to(
        {'test_CustomFragment': {'num': 42}},
        *[x.id for x in root.child_assets],
        *[x.id for x in root.child_devices],
        *[x.id for x in root.child_additions],
    )

    assert not notified.is_set()

    # (99) cleanup
    listener.close()
    listener_thread.join()


@pytest.mark.asyncio(loop_scope='function')
async def test_asyncio_child_updates(live_c8y: CumulocityApi, object_tree_builder):
    """Verify that updates to child objects are ignored."""
    root = object_tree_builder()

    root_subscription = Subscription(
        live_c8y,
        name=f'{root.name.replace("_", "")}Subscription',
        context=Subscription.Context.MANAGED_OBJECT, source_id=root.id
    ).create()

    # prepare listener
    notifications:list[AsyncListener.Message] = []
    async def receive_notification(m:AsyncListener.Message):
        notifications.append(m)
        await m.ack()

    listener = AsyncListener(live_c8y, subscription_name=root_subscription.name)
    listener_task = asyncio.create_task(listener.listen(receive_notification))
    await asyncio.sleep(5)  # ensure creation

    # update all child objects
    live_c8y.inventory.apply_to(
        {'test_CustomFragment': {'num': 42}},
        *[x.id for x in root.child_assets],
        *[x.id for x in root.child_devices],
        *[x.id for x in root.child_additions],
    )

    assert not notifications

    # (99) cleanup
    await listener.close()
    await listener_task


def test_parent_updates(live_c8y: CumulocityApi, object_tree_builder):
    """Verify that updates to parent objects are ignored."""
    root = object_tree_builder()

    children = root.child_assets + root.child_devices + root.child_additions
    child_subscriptions = [
        Subscription(
            live_c8y,
            name=f'{c.name.replace("_", "")}Subscription',
            context=Subscription.Context.MANAGED_OBJECT, source_id=c.id
        ).create()
        for c in children
    ]

    # prepare listeners
    notifications:list[Listener.Message] = []
    def receive_notification(m:Listener.Message):
        notifications.append(m)
        m.ack()

    listeners = [Listener(live_c8y, subscription_name=s.name) for s in child_subscriptions]
    listener_threads = [threading.Thread(target=l.listen, args=[receive_notification]) for l in listeners]
    for l in listener_threads:
        l.start()
    time.sleep(5)  # ensure creation

    # update all child objects
    root.apply({'test_CustomFragment': {'num': 42}})

    assert not notifications

    # (99) cleanup
    for l in listeners:
        l.close()
    for l in listener_threads:
        l.join()


@pytest.mark.asyncio(loop_scope='function')
async def test_asyncio_parent_updates(live_c8y: CumulocityApi, object_tree_builder):
    """Verify that updates to parent objects are ignored."""
    root = object_tree_builder()

    children = root.child_assets + root.child_devices + root.child_additions
    child_subscriptions = [
        Subscription(
            live_c8y,
            name=f'{c.name.replace("_", "")}Subscription',
            context=Subscription.Context.MANAGED_OBJECT, source_id=c.id
        ).create()
        for c in children
    ]

    # prepare listeners
    notifications:list[AsyncListener.Message] = []
    async def receive_notification(m:AsyncListener.Message):
        notifications.append(m)
        await m.ack()

    listeners = [AsyncListener(live_c8y, subscription_name=s.name) for s in child_subscriptions]
    listener_tasks = [asyncio.create_task(l.listen(receive_notification)) for l in listeners]
    await asyncio.sleep(5)  # ensure creation

    # update all child objects
    root.apply({'test_CustomFragment': {'num': 42}})

    assert not notifications

    # (99) cleanup
    await asyncio.gather(*[l.close() for l in listeners])
    await asyncio.wait(listener_tasks)


def build_managed_object_subscription(mo):
    """Build a subscription for a managed object."""
    return Subscription(
        name=f'{mo.name.replace("_", "")}Subscription',
        context=Subscription.Context.MANAGED_OBJECT, source_id=mo.id
    )

def create_managed_object_subscription(c8y, mo):
    """Build and create subscription for a managed object."""
    s = build_managed_object_subscription(mo)
    s.c8y = c8y
    return s.create()


def test_multiple_subscribers(live_c8y: CumulocityApi, sample_object):
    """Verify that multiple subscribers/consumers can be created for a single subscription.

    This test creates a managed object and corresponding subscription as well as multiple
    listeners with unique subscriber names. An update to the managed object should notify
    each of the subscribers.
    """

    mo = sample_object
    sub = create_managed_object_subscription(live_c8y, mo)

    # prepare listeners
    notifications:list[Listener.Message] = []
    def receive_notification(m:Listener.Message):
        notifications.append(m)
        m.ack()

    n_listeners = 3
    listeners = [Listener(live_c8y, subscription_name=sub.name, subscriber_name=f"{sub.name}{i}")
                 for i in range(n_listeners)]
    listener_threads = [threading.Thread(target=l.listen, args=[receive_notification]) for l in listeners]
    for l in listener_threads:
        l.start()
    time.sleep(5)  # ensure creation

    # update the object
    mo.apply({'test_CustomFragment': {'num': 42}})
    time.sleep(5)  # ensure processing
    # -> all received notifications are identical
    assert len(notifications) == 3
    assert len({n.raw for n in notifications}) == 1

    # (99) cleanup
    for l in listeners:
        l.close()
    for l in listener_threads:
        l.join()


@pytest.mark.asyncio(loop_scope='function')
async def test_asyncio_multiple_subscribers(live_c8y: CumulocityApi, sample_object):
    """Verify that multiple subscribers/consumers can be created for a single subscription.

    This test creates a managed object and corresponding subscription as well as multiple
    listeners with unique subscriber names. An update to the managed object should notify
    each of the subscribers.
    """

    mo = sample_object
    sub = create_managed_object_subscription(live_c8y, mo)

    # prepare listeners
    notifications:list[AsyncListener.Message] = []
    async def receive_notification(m:AsyncListener.Message):
        notifications.append(m)
        await m.ack()

    n_listeners = 3
    listeners = [AsyncListener(live_c8y, subscription_name=sub.name, subscriber_name=f"{sub.name}{i}")
                 for i in range(n_listeners)]
    listener_tasks = [asyncio.create_task(l.listen(receive_notification)) for l in listeners]
    await asyncio.sleep(5)  # ensure creation

    # update the object
    mo.apply({'test_CustomFragment': {'num': 42}})
    await asyncio.sleep(5)  # ensure processing
    # -> all received notifications are identical
    assert len(notifications) == 3
    assert len({n.raw for n in notifications}) == 1

    # (99) cleanup
    await asyncio.gather(*[l.close() for l in listeners])
    await asyncio.wait(listener_tasks)
