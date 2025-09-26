# Copyright (c) 2025 Cumulocity GmbH

from concurrent.futures import wait, Future
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
import logging
import threading
import time
from typing import Callable, Union, List

from c8y_api.app import MultiTenantCumulocityApp


class SubscriptionListener:
    """Multi-tenant subscription listener.

    When implementing a multi-tenant microservice it is sometimes required to
    keep track of the tenants which subscribe to the microservice.
    Effectively, this needs to be done via polling the `get_subscribers`
    function of the MultiTenantCumulocityApp class.

    The `SubscriptionListener` class provides a default implementation of
    such a polling mechanism which can be easily integrated using callbacks.
    """

    # instance counter to ensure unique loggers
    _n = 0

    def __init__(
            self,
            app: MultiTenantCumulocityApp,
            callback: Callable[[List[str]], None] = None,
            max_threads: int = 5,
            blocking: bool = True,
            polling_interval: float = 3600,
            startup_delay: float = 60,
    ):
        """Create and initialize a new instance.

        See also the `add_callback` function which can be used to add callbacks
        in a more fine-granular fashion.

        Args:
            app (MultiTenantCumulocityApp):  The microservice app instance
            callback (Callable):  A callback to be invoked when another tenant
                subscribes or unsubscribes from the microservice; The function
                will be invoked with the current list of subscribers.
            blocking (bool):  Whether the `callback` function will be invoked
                in blocking fashion (True, default) or detached in a thread
                (False).
            polling_interval (float):  The polling interval
            startup_delay (float):  A minimum delay before a newly added
                microservice is considered to be "added" (the callback
                invocation will be delayed by this).
        """
        instance_id = f'[{self._n}]' if self._n > 1 else ''
        self._n = self._n + 1
        self._instance_name = type(self).__name__ + instance_id
        self.app = app
        self.max_threads = max_threads
        self.startup_delay = startup_delay
        self.polling_interval = polling_interval
        self.callbacks = [(callback, blocking)] if callback else []
        self.callbacks_on_add = []
        self.callbacks_on_remove = []
        self._log = logging.getLogger(__name__ + instance_id)
        self._executor = None
        self._callback_futures = set()
        self._listen_thread = None
        self._is_closed = threading.Event()

    def _cleanup_future(self, future):
        """Remove a finished future from the internal list."""
        self._callback_futures.remove(future)

    def add_callback(
            self,
            callback: Callable[[Union[str, List[str]]], None],
            blocking: bool = True,
            when: str = 'any'
    ) -> "SubscriptionListener":
        """Add a callback function to be invoked if a tenant subscribes
        to/unsubscribes from the monitored multi-tenant microservice.

        Note: multiple callbacks (even listening to the same event) can
        be defined. The `add_callback` function supports a fluent interface,
        i.e. it can be chained, to ease configuration.

        Args:
             callback (Callable):  A callback function to invoke in case
                of a change in subscribers. If parameter `when` is either
                "added" or "removed" the function is invoked with a single
                tenant ID for every added/removed subscriber respectively.
                Otherwise (or if "always/any"), the callback function is
                invoked with a list of the current subscriber's tenant IDs.
            blocking (bool):  Whether to invoke the callback function in a
                blocking fashion (default) or not. If False, a thread is
                spawned for each invocation.
            when (str):  When to invoke this particular callback function.
                If "added" or "removed" the callback function is invoked with
                a single tenant ID for every added/removed subscriber
                respectively. Otherwise (or if "always/any"), the callback
                function is invoked with a list of the current subscriber's
                tenant IDs.
        """
        if when in {'always', 'any'}:
            self.callbacks.append((callback, blocking))
            return self
        if when == 'added':
            self.callbacks_on_add.append((callback, blocking))
            return self
        if when == 'removed':
            self.callbacks_on_remove.append((callback, blocking))
            return self
        raise ValueError(f"Invalid activation mode: {when}")

    def listen(self):
        """Start the listener.

        This is blocking.
        """
        # pylint: disable=too-many-branches

        # safely invoke a callback function blocking or non-blocking
        def invoke_callback(callback, is_blocking, _, arg):
            def safe_invoke(a):
                # pylint: disable=broad-exception-caught
                try:
                    if self._log.isEnabledFor(logging.DEBUG):
                        self._log.debug(f"Invoking callback: {callback.__module__}.{callback.__name__}")
                    callback(a)
                except Exception as callback_error:
                    self._log.error(f"Uncaught exception in callback: {callback_error}", exc_info=callback_error)
            if is_blocking:
                safe_invoke(arg)
            else:
                future = self._executor.submit(safe_invoke, arg)
                self._callback_futures.add(future)
                future.add_done_callback(self._cleanup_future)

        try:  # entire listen block encapsulated in try/except

            self._log.debug("Listener started.")

            # create an executor if there are non-blocking callbacks
            if any(not x[1] for x in (*self.callbacks, *self.callbacks_on_add, *self.callbacks_on_remove)):
                self._log.debug("Setting up executor for non-blocking callbacks.")
                self._executor = ThreadPoolExecutor(
                    max_workers=self.max_threads,
                    thread_name_prefix=self._instance_name)

            last_subscribers = set()
            while not self._is_closed.is_set():
                now = time.monotonic()
                # read & check current subscribers
                self._log.debug("Checking current subscriber list.")
                current_subscribers = set(self.app.get_subscribers())
                added = current_subscribers - last_subscribers
                removed = last_subscribers - current_subscribers
                # run 'removed' callbacks
                for tenant_id in removed:
                    self._log.info(f"Tenant subscription removed: {tenant_id}.")
                    for fun, blocking in self.callbacks_on_remove:
                        invoke_callback(fun, blocking, 'Removed', tenant_id)
                # wait remaining time for startup delay
                if added and self.startup_delay:
                    min_startup_delay = self.startup_delay - (time.monotonic() - now)
                    if min_startup_delay > 0:
                        time.sleep(min_startup_delay)
                # run 'added' callbacks
                for tenant_id in added:
                    self._log.info(f"Tenant subscription added: {tenant_id}.")
                    for fun, blocking in self.callbacks_on_add:
                        invoke_callback(fun, blocking, 'Added', tenant_id)
                # run 'any' callbacks
                if added or removed:
                    self._log.info(f"Current subscriptions: {', '.join(current_subscribers) or 'None'}.")
                    for fun, blocking in self.callbacks:
                        invoke_callback(fun, blocking, None, current_subscribers)
                # set new baseline
                last_subscribers = current_subscribers
                # schedule next run, skip if already exceeded
                next_run = time.monotonic() + self.polling_interval
                if self._log.isEnabledFor(logging.DEBUG):
                    next_run_datetime = datetime.now(timezone.utc) + timedelta(seconds=self.polling_interval)
                    self._log.debug(f"Next run at {next_run_datetime.isoformat(sep=' ', timespec='seconds')}.")
                # sleep until next poll
                if not time.monotonic() > next_run:
                    self._is_closed.wait(next_run - time.monotonic())
                else:
                    time.sleep(0.1)  # release GIL

            # shutdown executor, but don't wait for the callbacks
            if self._executor:
                self._executor.shutdown(wait=False, cancel_futures=False)

        # pylint: disable=broad-exception-caught
        except Exception as error:
            self._log.error(f"Uncaught exception during listen: {error}", exc_info=error)

        self._log.debug("Listener ended.")

    def start(self) -> threading.Thread:
        """Start the listener in a separate thread.

        This function will return immediately. The listening can be stopped
        using the `shutdown` function.

        Returns:
            The created Thread.
        """
        self._listen_thread = threading.Thread(target=self.listen, name=f"{self._instance_name}_Main")
        self._listen_thread.start()
        return self._listen_thread

    def stop(self):
        """Signal to stop the listening thread.

        This function returns immediately; neither the completion of the
        `listen` function, nor potentially running callbacks are awaited.
        Use this, if the `listen` function is running in a thread managed
        by your code.

        See also:
            Function `await_callbacks`, to await the completion of potentially
            running callback functions.
            Functions `start` and `shutdown` if you don't want to manage the
            listening thread on your own.
        """
        self._is_closed.set()

    def shutdown(self, timeout: float = None):
        """Shutdown the listener thread and wait for it to finish.

        This function can only be invoked if the listener thread was started
        using the `start` function (i.e. the thread is managed by this class).
        Otherwise, the `stop` function should be used.

        Args:
            timeout (float):  Maximum wait time (None to wait indefinitely).

        Raises:
            TimeoutError, if the shutdown could not complete within the
                specified timeout. The shutdown procedure is not interrupted
                by this and will complete eventually.
        """
        if not self._listen_thread:
            raise RuntimeError("Listener thread is maintained elsewhere. Nothing to do.")
        self.stop()
        # wait for listen thread
        start = time.monotonic()
        self._listen_thread.join(timeout=timeout)
        # wait for callbacks if there is time
        if not timeout:
            self.await_callbacks()
        else:
            remaining = timeout - (time.monotonic() - start)
            if remaining > 0:
                self.await_callbacks(timeout=remaining)
        # raise timeout error if not complete
        if self._listen_thread.is_alive() or (
                self._executor and self.get_callbacks()):
            raise TimeoutError(f"Listener thread did not close within the specified timeout ({timeout}s).")

    def get_callbacks(self) -> List[Future]:
        """Get currently running callbacks.

        This function can be used to gain direct access to the currently
        running callback threads. Usually, this is not necessary.

        See also:
             Function `await_callbacks` to await the completion of all
             currently running callback threads.
        """
        return [f for f in self._callback_futures if f.running()]

    def await_callbacks(self, timeout: float = None):
        """Await running callbacks.

        Args:
            timeout (float): Maximum wait time (None to wait indefinitely)

        Raises:
            TimeoutError, if there are still running callbacks after the
                specified timeout.
        """
        wait(self._callback_futures, timeout=timeout)
        if self.get_callbacks():
            raise TimeoutError(f"Callback functions did not complete within the specified timeout ({timeout}s).")
