# Copyright (c) 2025 Cumulocity GmbH

import math
import time
from unittest.mock import Mock

import pytest

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
            listener.stop()

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
    removed_fun = Mock(side_effect=lambda _: listener.stop())

    app.get_subscribers = Mock(side_effect=[
        ['t1'],  # add
        [], # remove
    ])

    listener.add_callback(added_fun, when='added', blocking=False)
    listener.add_callback(removed_fun, when='removed', blocking=False)

    # listen and await running threads
    listener.listen()
    listener.await_callbacks()
    # -> there should be no running threads
    time.sleep(0.1)  # release GIL
    assert not listener.get_callbacks()
    # -> callback function should have been invoked
    added_fun.assert_called_with('t1')
    removed_fun.assert_called_with('t1')
    assert listener._cleanup_future.call_count == 2


def test_long_running_callback_threads():
    """Verify that long running callback threads do not mess up production.

    This test simulates a set of "added" subscribers which is larger than the
    number of max threads. Still, all created threads can finish successfully.
    """
    app:MultiTenantCumulocityApp = Mock(spec=MultiTenantCumulocityApp)
    listener = SubscriptionListener(app=app, polling_interval=0.1, startup_delay=0, max_threads=2)
    # mock-wrap cleanup function for monitoring
    listener._cleanup_future = Mock(side_effect=lambda future: SubscriptionListener._cleanup_future(listener, future))

    added_fun = Mock(side_effect=Mock(side_effect=lambda _: (listener.stop(), time.sleep(1))))
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
    assert listener.get_callbacks()
    assert all(t.running() for t in listener.get_callbacks())
    # -> but no more than max threads
    assert len(listener.get_callbacks()) <= listener.max_threads

    # await threads
    listener.await_callbacks()
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
    which immediately closes the listener and preserves the call time. The
    'listen' function would not be exited immediately.
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
        listener.stop()

    listener.add_callback(added_fun, when='added')

    # ensure there is a set of added subscribers
    app.get_subscribers = Mock(return_value=['t1'])

    # listen and await running threads
    # - t1 is set in callback function
    t0 = time.monotonic()
    listener.listen()
    listener.await_callbacks()
    # -> t1 includes the startup delay
    assert t1 - t0 > listener.startup_delay


def test_listener_thread():
    """Verify that the start/shutdown functions will spawn and terminate a
    listener thread as expected.

    This test mocks the `listen` function as only the thread wrapper is
    tested. The `start` function spawns a thread (returned) and the
    `shutdown` function will terminate the thread but wait for the `listen`
    function to complete.
    """
    listen_run_time = 3

    # create class under test
    app:MultiTenantCumulocityApp = Mock(spec=MultiTenantCumulocityApp)
    listener = SubscriptionListener(app=app, polling_interval=0.1)
    listener.listen = Mock(side_effect=lambda: time.sleep(listen_run_time))

    # start listener thread
    t0 = time.monotonic()
    listener_thread = listener.start()
    # -> thread is alive
    assert listener_thread.is_alive()
    # -> listen function is called
    listener.listen.assert_called()

    # shutdown listener thread
    listener.shutdown()
    # -> thread is no longer alive
    assert not listener_thread.is_alive()
    # -> shutdown waited for listen to complete
    t1 = time.monotonic()
    assert t1-t0 > listen_run_time


def test_listener_thread_timeout():
    """Verify that the start/shutdown functions will spawn a listener thread
    as expected and doesn't wait for termination when a timeout is specified.

    This test mocks the `listen` function as only the thread wrapper is
    tested. The `start` function spawns a thread (returned) and the
    `shutdown` function will start the termination but won't wait for
    the `listen` function to complete. A TimeoutError is raised as the
    timeout hit.
    """

    listen_run_time = 3

    # create class under test
    app:MultiTenantCumulocityApp = Mock(spec=MultiTenantCumulocityApp)
    listener = SubscriptionListener(app=app, polling_interval=0.1)
    listener.listen = Mock(side_effect=lambda: time.sleep(listen_run_time))

    # start listener thread
    t0 = time.monotonic()
    listener_thread = listener.start()
    # -> thread is alive
    assert listener_thread.is_alive()
    # -> listen function is called
    listener.listen.assert_called()

    # shutdown listener thread
    with pytest.raises(TimeoutError):
        listener.shutdown(timeout=0.1)
    # -> thread is still alive
    assert listener_thread.is_alive()
    # -> shutdown didn't wait for listen to complete
    t1 = time.monotonic()
    assert t1-t0 < listen_run_time

    # wait for listen function
    time.sleep(listen_run_time)
    assert not listener_thread.is_alive()


@pytest.mark.parametrize("blocking", [True, False])
def test_listener_thread_waiting(blocking):
    import logging
    logging.basicConfig(level=logging.DEBUG)

    callback_run_time = 3

    # create class under test
    app:MultiTenantCumulocityApp = Mock(spec=MultiTenantCumulocityApp)
    listener = SubscriptionListener(app=app, polling_interval=0.1, startup_delay=0)

    # add callback
    fun_mock = Mock(side_effect=lambda _: time.sleep(callback_run_time))
    listener.add_callback(callback=fun_mock, blocking=blocking)

    # ensure there is a set of added subscribers
    app.get_subscribers = Mock(return_value=['t1'])

    t0 = time.monotonic()
    # start listener thread and shutdown but don't wait
    listener_thread = listener.start()
    with pytest.raises(TimeoutError):
        listener.shutdown(timeout=0.1)
    # -> thread is still alive (if blocking)
    if blocking:
        assert listener_thread.is_alive()

    # wait for listener thread to complete
    listener_thread.join()
    listener.await_callbacks()
    t1 = time.monotonic()
    # -> we waited for the callback to complete
    assert t1-t0 > callback_run_time
    assert t1-t0 < callback_run_time*2
    fun_mock.assert_called_once_with({'t1'})


def test_multiple_threads_in_parallel():
    """Assert that non-blocking threads run in parallel."""
    app:MultiTenantCumulocityApp = Mock(spec=MultiTenantCumulocityApp)
    listener = SubscriptionListener(app=app, max_threads=3, polling_interval=0.1, startup_delay=0)

    callback_run_time = 3

    fun_mock1 = Mock(side_effect=lambda _: time.sleep(callback_run_time))
    fun_mock2 = Mock(side_effect=lambda _: time.sleep(callback_run_time))
    fun_mock3 = Mock(side_effect=lambda _: time.sleep(callback_run_time))
    fun_mock4 = Mock(side_effect=lambda _: time.sleep(callback_run_time))

    # (1) 3 callbacks to be invoked
    listener.add_callback(callback=fun_mock1, blocking=False)
    listener.add_callback(callback=fun_mock2, blocking=False)
    listener.add_callback(callback=fun_mock3, blocking=False)
    app.get_subscribers = Mock(return_value=['t1'])

    # start listener thread and shutdown
    t0 = time.monotonic()
    listener.start()
    listener.shutdown()
    t1 = time.monotonic()
    # -> all callbacks ran in parallel
    assert callback_run_time < t1 - t0 < callback_run_time + 0.2


def test_too_many_multiple_threads():
    """Assert that when the number of threads exceed the executors capacity,
    the invocations will be queued."""
    app:MultiTenantCumulocityApp = Mock(spec=MultiTenantCumulocityApp)
    listener = SubscriptionListener(app=app, max_threads=3, polling_interval=0.1, startup_delay=0)
    callback_run_time = 2

    # 3+1 callbacks to be invoked
    fun_mock = Mock(side_effect=lambda _: time.sleep(callback_run_time))
    listener.add_callback(callback=fun_mock, blocking=False)
    listener.add_callback(callback=fun_mock, blocking=False)
    listener.add_callback(callback=fun_mock, blocking=False)
    listener.add_callback(callback=fun_mock, blocking=False)
    app.get_subscribers = Mock(return_value=['t1'])

    # start listener thread and shutdown
    t0 = time.monotonic()
    listener.start()
    listener.shutdown()
    t1 = time.monotonic()
    # -> all callbacks but one ran in parallel
    assert callback_run_time*2 < t1 - t0 < callback_run_time*2 + 0.2
