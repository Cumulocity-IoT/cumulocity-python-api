# Copyright (c) 2025 Cumulocity GmbH

import logging
import time
from concurrent.futures import wait
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Callable, Union

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
            callback: Callable[[list[str]], None] = None,
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
        self._n = self._n + 1
        self._instance_name = f"{__name__}.{type(self).__name__}[{self._n}]"
        self.app = app
        self.max_threads = max_threads
        self.startup_delay = startup_delay
        self.polling_interval = polling_interval
        self.callbacks = [(callback, blocking)] if callback else []
        self.callbacks_on_add = []
        self.callbacks_on_remove = []
        self._log = logging.Logger(self._instance_name)
        self._executor = None
        self._callback_futures = set()
        self._is_closed = False

    def _cleanup_future(self, future):
        """Remove a finished future from the internal list."""
        self._callback_futures.remove(future)

    def add_callback(
            self,
            callback: Callable[[Union[str ,list[str]]], None],
            blocking: bool = True,
            when: str = 'any',
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
        # invoke a callback function
        def invoke_callback(callback, is_blocking, arg):
            def safe_invoke(a):
                try:
                    callback(a)
                except Exception as error:
                    print(f"Uncaught exception in callback: {error}")
                    self._log.error(f"Uncaught exception in callback: {error}", exc_info=error)
            if is_blocking:
                safe_invoke(arg)
            else:
                future = self._executor.submit(safe_invoke, arg)
                future.add_done_callback(self._cleanup_future)
                self._callback_futures.add(future)

        if any(not x[1] for x in (*self.callbacks_on_add, *self.callbacks_on_remove)):
            self._executor = ThreadPoolExecutor(max_workers=self.max_threads, thread_name_prefix=self._instance_name)
        last_subscribers = set()
        next_run = 0
        while not self._is_closed:
            # sleep until next poll
            now = time.monotonic()
            if not now > next_run:
                time.sleep(next_run - now)
            # read subscribers
            current_subscribers = set(self.app.get_subscribers())
            added = current_subscribers - last_subscribers
            removed = last_subscribers - current_subscribers
            # # clean threads
            # if self._running_threads:
            #     self._running_threads = [t for t in self._running_threads if t.is_alive()]
            # run 'removed' callbacks
            for tenant_id in removed:
                for fun, blocking in self.callbacks_on_remove:
                    invoke_callback(fun, blocking, tenant_id)
            # wait remaining time for startup delay
            if added and self.startup_delay:
                time.sleep(time.monotonic() - now + self.startup_delay)
            # run 'added' callbacks
            for tenant_id in added:
                for fun, blocking in self.callbacks_on_add:
                    invoke_callback(fun, blocking, tenant_id)
            # run 'any' callbacks
            for fun, blocking in self.callbacks:
                    invoke_callback(fun, blocking, current_subscribers)
            # set new baseline
            last_subscribers = current_subscribers
            # schedule next run, skip if already exceeded
            next_run = time.monotonic() + self.polling_interval
            # release GIL
            time.sleep(0.1)

        if self._executor:
            self._executor.shutdown(wait=False)

    def stop(self):
        self._is_closed = True

    def get_callback_threads(self):
        # pylint: disable=protected-access
        return [t for t in self._executor._threads if t.is_alive()]

    def await_callback_threads(self, timeout: int = None):
        wait(self._callback_futures, timeout=timeout)
