# Copyright (c) 2025 Cumulocity GmbH

import asyncio

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Device, ManagedObject, Subscription
from c8y_tk.notification2 import AsyncListener

from util.testing_util import RandomNameGenerator


def test_subscription_deletion(live_c8y, register_object):
    """Verify that a subscription is removed with the corresponding managed object."""
    mo_name = RandomNameGenerator.random_name(3)
    mo = register_object(ManagedObject(live_c8y, name=f'{mo_name}1', type=f'test_{mo_name}').create())

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


@pytest.mark.asyncio(loop_scope='function')
async def test_asyncio_object_update_and_deletion(live_c8y: CumulocityApi, safe_create):
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

    notifications:list[AsyncListener.Message] = []
    async def receive_notification(m:AsyncListener.Message):
        notifications.append(m)
        await m.ack()

    listener = AsyncListener(live_c8y, subscription_name=sub.name)
    listener_task = asyncio.create_task(listener.listen(receive_notification))
    await asyncio.sleep(5)  # ensure creation

    # (1) apply first change, expected fragment
    mo.apply({'test_AwaitedFragment': {'num': 42}})
    await asyncio.sleep(5)  # ensure processing
    # -> notification should appear
    assert len(notifications) == 1
    assert mo.id in notifications[0].source
    assert notifications[0].action == "UPDATE"
    # -> basic data AND expected fragment in payload
    assert notifications[0].json['id'] == mo.id
    assert notifications[0].json['name'] == mo.name
    assert notifications[0].json['type'] == mo.type
    assert notifications[0].json['test_AwaitedFragment']['num'] == 42

    # (2) Apply 2nd change, different fragment
    notifications = []
    mo.apply({'test_DifferentFragment': {'num': 42}})
    await asyncio.sleep(5)  # ensure processing
    # -> notification should appear
    assert len(notifications) == 1
    assert mo.id in notifications[0].source
    assert notifications[0].action == "UPDATE"
    # -> basic data in payload
    assert notifications[0].json['id'] == mo.id
    assert notifications[0].json['name'] == mo.name
    assert notifications[0].json['type'] == mo.type
    # -> other fragment not in payload
    assert 'test_AwaitedFragment' in notifications[0].json
    assert 'test_DifferentFragment' not in notifications[0].json

    # (3) delete object tree
    notifications = []
    mo.delete_tree()
    await asyncio.sleep(5)  # ensure processing
    # -> notification should appear
    assert len(notifications) == 1
    assert mo.id in notifications[0].source
    assert notifications[0].action == "DELETE"

    # (99) cleanup
    await listener.close()
    await listener_task


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


@pytest.mark.asyncio(loop_scope='function')
async def test_multiple_subscribers(live_c8y: CumulocityApi, sample_object):
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
    assert all(n.raw == notifications[0].raw for n in notifications)

    # (99) cleanup
    await asyncio.gather(*[l.close() for l in listeners])
    await asyncio.wait(listener_tasks)
