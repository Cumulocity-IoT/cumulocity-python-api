# Copyright (c) 2025 Cumulocity GmbH

import math
import time
from unittest.mock import Mock

from c8y_api.app import MultiTenantCumulocityApp
from c8y_tk.app import SubscriptionListener


def test_sub_setting():
    """Verify that the callback functions are invoked as expected.

    This test mocks the get_subscribers function to emit results equivalent
    to various tenants being added or removed. The parameters of the callback
    functions are preserved and asserted after test.
    """
    app:MultiTenantCumulocityApp = Mock(spec=MultiTenantCumulocityApp)
    listener = SubscriptionListener(app=app, polling_interval=0, startup_delay=0)

    added = set()
    def added_fun(tenant_id):
        added.add(tenant_id)

    removed = set()
    def removed_fun(tenant_id):
        nonlocal listener
        removed.add(tenant_id)
        if tenant_id == 't2':
            listener.close()

    always = []
    def always_fun(tenant_ids):
        always.append(tenant_ids)

    app.get_subscribers = Mock(side_effect=[
        ['t1'],  # add t1
        ['t1', 't2', 't3'],  # add t2, t3
        ['t2', 't3', 't4'],  # remove t1, add t4
        ['t4'], # remove t2, t3
    ])
    listener.add_callback(added_fun, when='added')
    listener.add_callback(removed_fun, when='removed')
    listener.add_callback(always_fun)

    listener.listen()

    assert added == {'t1', 't2', 't3', 't4'}
    assert removed == {'t1', 't2', 't3'}
    assert always == [
        {'t1'},
        {'t1', 't2', 't3'},
        {'t2', 't3', 't4'},
        {'t4'},
    ]


def test_callback_threads():
    """Verify that non-blocking callbacks are executed as expected.

    This test simply verifies that threads and corresponding futures are
    created and cleaned up properly. It creates mocks for the callback
    functions and wraps the cleanup function in a mock to assert the correct
    invocation.
    """
    app:MultiTenantCumulocityApp = Mock(spec=MultiTenantCumulocityApp)
    listener = SubscriptionListener(app=app, polling_interval=0, startup_delay=0)
    # mock-wrap cleanup function for monitoring
    listener._cleanup_future = Mock(side_effect=lambda future: SubscriptionListener._cleanup_future(listener, future))
    added_fun = Mock()
    removed_fun = Mock(side_effect=lambda _: listener.close())

    app.get_subscribers = Mock(side_effect=[
        ['t1'],  # add
        [], # remove
    ])

    listener.add_callback(added_fun, when='added', blocking=False)
    listener.add_callback(removed_fun, when='removed', blocking=False)

    # listen and await running threads
    listener.listen()
    listener.await_callback_threads()
    # -> there should be no running threads
    assert not listener.get_callback_threads()
    # -> callback function should have been invoked
    added_fun.assert_called_with('t1')
    removed_fun.assert_called_with('t1')
    assert listener._cleanup_future.call_count == 4


def test_long_running_callback_threads():
    """Verify that long running callback threads do not mess up production.

    This test simulates a set of "added" subscribers which is larger than the
    number of max threads. Still, all created threads can finish successfully.
    """
    app:MultiTenantCumulocityApp = Mock(spec=MultiTenantCumulocityApp)
    listener = SubscriptionListener(app=app, polling_interval=0.1, startup_delay=0, max_threads=2)
    # mock-wrap cleanup function for monitoring
    listener._cleanup_future = Mock(side_effect=lambda future: SubscriptionListener._cleanup_future(listener, future))

    added_fun = Mock(side_effect=Mock(side_effect=lambda _: (listener.close(), time.sleep(1))))
    listener.add_callback(added_fun, when='added', blocking=False)

    # ensure there is a set of added subscribers
    app.get_subscribers = Mock(return_value=['t1', 't2', 't3', 't4'])

    # listen and await running threads
    t0 = time.monotonic()
    listener.listen()
    t1 = time.monotonic()
    # -> listen should exit immediately (because callback closes)
    assert t1 - t0 < 1
    # -> there should be still running threads
    assert listener.get_callback_threads()
    assert all(t.is_alive() for t in listener.get_callback_threads())
    # -> but no more than max threads
    assert len(listener.get_callback_threads()) <= listener.max_threads

    # await threads
    listener.await_callback_threads()
    t2 = time.monotonic()
    # -> callbacks have been executed in parallel
    num_thread_groups = math.ceil(added_fun.call_count / listener.max_threads)
    assert num_thread_groups < t2 - t0 < num_thread_groups + 1
    # -> callback function should have been invoked
    assert added_fun.call_count == len(app.get_subscribers())
    assert listener._cleanup_future.call_count == added_fun.call_count


def test_startup_delay():
    """Verify that a startup delay is honored as expected.

    This test defines a listener with startup delay and adds a callback mock
    which immediately closes the listener and preserves the call time. The 'listen' function would not
    be exited immediately

    """
    # create class under test
    app:MultiTenantCumulocityApp = Mock(spec=MultiTenantCumulocityApp)
    listener = SubscriptionListener(app=app, polling_interval=0.1, startup_delay=1)
    # mock-wrap cleanup function for monitoring
    listener._cleanup_future = Mock(side_effect=lambda future: SubscriptionListener._cleanup_future(listener, future))

    t0 , t1 = 0, 0
    def added_fun(_):
        nonlocal t1, listener
        t1 = time.monotonic()
        listener.close()

    listener.add_callback(added_fun, when='added')

    # ensure there is a set of added subscribers
    app.get_subscribers = Mock(return_value=['t1'])

    # listen and await running threads
    # - t1 is set in callback function
    t0 = time.monotonic()
    listener.listen()
    listener.await_callback_threads()
    # -> t1 includes the startup delay
    assert t1 - t0 > listener.startup_delay
