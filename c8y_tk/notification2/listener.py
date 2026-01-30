# Copyright (c) 2025 Cumulocity GmbH

import asyncio
import contextlib
import json as js
import logging
import ssl
import threading
import uuid
from itertools import count
import queue as sync_queue
from typing import Callable, Awaitable

import certifi
from urllib3.exceptions import SSLError

try:
    import websockets.asyncio.client as ws_client
except ModuleNotFoundError:
    import websockets.client as ws_client
from websockets.exceptions import ConnectionClosed, InvalidStatus

from c8y_api.app import CumulocityApi
from c8y_api._jwt import JWT


class _Message(object):
    """Abstract base class for Notification 2.0 messages."""

    def __init__(self, payload: str):
        self.raw = payload
        parts = payload.splitlines(keepends=False)
        assert len(parts) > 3
        self.id = parts[0]
        self.source = parts[1]
        self.action = parts[2]
        self.body = parts[len(parts) - 1]

    @property
    def json(self):
        """JSON representation (dict) of the message body."""
        return js.loads(self.body)


class AsyncListener(object):
    """Asynchronous Notification 2.0 listener.

    Notification 2.0 events are distributed via Pulsar topics, communicating
    via websockets.

    This class encapsulates the Notification 2.0 communication protocol,
    providing a standard callback mechanism.

    Note: Listening with callback requires some sort of parallelism. This
    listener is implemented in a non-blocking fashion using Python coroutines.
    Class `Listener` implements the same functionality using a classic,
    blocking approach.

    See also: https://cumulocity.com/guides/reference/notifications/
    """

    _ids = count(0)

    class Message(_Message):
        """Represents a Notification 2.0 message.

        This class is intended to be used with class `AsyncListener` only.
        """

        def __init__(self, listener: "AsyncListener", payload: str):
            """Create a new Notification 2.0 message.

            Args:
                listener (AsyncListener):  Reference to the originating listener
                payload (str):  Raw message payload
            """
            super().__init__(payload)
            self.listener = listener

        async def ack(self):
            """Acknowledge the message."""
            await self.listener.ack(self.id)

    def __init__(
            self,
            c8y: CumulocityApi,
            subscription_name: str,
            subscriber_name: str = None,
            consumer_name: str = None,
            shared: bool = False,
            auto_ack: bool = True,
            auto_unsubscribe: bool = True,
    ):
        """Create a new Listener.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            subscription_name (str):  Subscription name
            subscriber_name (str): Subscriber (consumer) name; a sensible default
                is used when this is not defined.
        """
        self._id = next(self._ids)
        self._log = logging.getLogger(f'{__name__}.AsyncListener[{self._id}]')

        self.c8y = c8y
        self.subscription_name = subscription_name
        self.subscriber_name = subscriber_name or subscription_name
        self.consumer_name = consumer_name
        self.shared = shared
        self.auto_ack = auto_ack
        self.auto_unsubscribe = auto_unsubscribe
        self.signed_token = True
        self.token_validity = 1440
        self.ping_interval = 60
        self.ping_timeout = 20
        self.retry_interval = 0.1
        self.retry_rate = 1.5
        self.retry_max_delay = 30

        self._task = None
        self._connection = None
        self._current_token = None
        self._is_running = asyncio.Event()
        self._is_connected = asyncio.Event()
        self._stop_event = asyncio.Event()


    def _create_token(self) -> str:
        token = self.c8y.notification2_tokens.generate(
            subscription=self.subscription_name,
            subscriber=self.subscriber_name,
            shared=self.shared,
            signed=self.signed_token,
            expires=self.token_validity,
        )
        self._log.info(
            "New Notification 2.0 token requested for subscription %s, %s.",
            self.subscription_name,
            self.subscriber_name,
        )
        return token

    # Note: Return type naming differs for different Python Versions; ClientConnection
    # refers to the latest module revision
    async def _create_connection(self)-> ws_client.ClientConnection:
        self._current_token = self._create_token()
        # if shared, consumer names should be unique
        consumer = self.consumer_name  # user's choice is used
        if not consumer and self.shared:
            consumer = self.subscriber_name + uuid.uuid4().hex[:8]
        # a consumer name is used for shared subscribers, only
        uri = self.c8y.notification2_tokens.build_websocket_uri(
            token=self._current_token,
            consumer=consumer if self.shared else None,
        )
        # ensure that the SSL context uses certifi
        ssl_context = None
        if self.c8y.is_tls:
            ssl_context = ssl.create_default_context()
            ssl_context.load_verify_locations(certifi.where())
        connection = await ws_client.connect(
            uri=uri,
            ping_interval=self.ping_interval,
            ping_timeout=self.ping_timeout,
            ssl=ssl_context
        )
        self._log.info(
            "Websocket connection established for subscription %s, %s.",
            self.subscription_name,
            self.subscriber_name,
        )
        return connection

    async def listen(self, callback: Callable[["AsyncListener.Message"], Awaitable[None]]):
        """Listen and handle messages.

        This function starts listening for new Notification 2.0 messages on
        the websocket channel. Each received message is wrapped in a `Message`
        object and forwarded to the callback function for handling.

        The messages are not automatically acknowledged. This can be done
        via the `Message` object's `ack` function by the callback function.

        Note: the callback function is invoked as a task and not awaited.

        This function will automatically handle the websocket communication
        including the authentication via tokens and reconnecting on
        connection loss. It will end when the listener is closed using its
        `close` function.
        """
        async def _callback(msg):
            try:
                await callback(msg)
                self._log.debug("Message %s processed.", msg.id)
                if self.auto_ack:
                    await msg.ack()
                    self._log.debug("Message %s acknowledged.", msg.id)
            except Exception as e:  # pylint: disable=broad-exception-caught
                self._log.error("Callback failed with exception: %s", e, exc_info=e)

        if self._is_running.is_set():
            raise RuntimeError("Listener already started")
        self._task = asyncio.current_task()
        self._is_running.set()
        self._stop_event.clear()

        # outer connection loop
        connection_retry = 0
        while not self._stop_event.is_set():
            try:
                self._connection = await self._create_connection()
                self._is_connected.set()
                connection_retry = 0  # reset after successful connect
                # inner receive loop
                while not self._stop_event.is_set():
                    payload = await self._connection.recv()
                    self._log.debug("Received message: %s", payload)
                    await asyncio.create_task(_callback(AsyncListener.Message(listener=self, payload=payload)))
            except asyncio.CancelledError:
                self._log.info("Subscriber %s cancelled. Stopping ...", self.subscriber_name)
                self._stop_event.set()
            except (ConnectionClosed, InvalidStatus) as e:
                self._log.info("Websocket connection failed: %s", e)
                connection_retry += 1
                backoff_delay = self.retry_interval * self.retry_rate ** connection_retry
                await asyncio.wait_for(self._stop_event.wait(), timeout=min(backoff_delay, self.retry_max_delay))
                continue  # reconnect via main loop
            except SSLError as e:
                self._log.error("SSL connection failed: %s", e, exc_info=e)
                raise e
            finally:
                # close and clear connection
                self._is_connected.clear()
                if self._connection:
                    with contextlib.suppress(Exception):
                        await self._connection.close()
                self._connection = None

        self._is_running.clear()
        if self.auto_unsubscribe:
            self.unsubscribe()

    def start(self, callback: Callable[["AsyncListener.Message"], Awaitable[None]]):
        """Start the listener.

        This function will start the listening process (`listen` function)
        and register the callback function to be invoked on every subscribed
        notification.

        Args:
            callback: Function to be invoked on notifications

        Returns:
            Created listener task.
        """
        return asyncio.create_task(self.listen(callback))

    def stop(self):
        """Signal the listener to be stopped."""
        self._task.cancel()  # raise CancelledError in listen task

    async def wait(self, timeout=None):
        """Wait for the listener task to finish.

        Args:
            timeout (int): The number of seconds to wait for the listener
                to finish. The listener will be cancelled if the timeout
                occurs.
        """
        if self._task:
            await asyncio.wait_for(self._task, timeout=timeout)

    def unsubscribe(self):
        """Unsubscribe the listener.

        Manually unsubscribing is required if the listener wasn't created
        with `auto_unsubscribe=True`.

        See also https://cumulocity.com/api/core/#section/Overview/Consumers-and-tokens
        """
        try:
            parsed_token = JWT(self._current_token)
            if parsed_token.get_valid_seconds() < 60:
                self._current_token = self._create_token()
            self.c8y.notification2_tokens.unsubscribe(self._current_token)
            self._log.info("Subscriber %s unsubscribed.", self.subscriber_name)
        except SyntaxError:
            if not self.shared:
                self._log.error(
                    "Subscriber %s could not be unsubscribed (potentially data leak).",
                    self.subscriber_name)
            else:
                self._log.info(
                    "Subscriber %s could not be unsubscribed (assuming it was already unsubscribed).",
                    self.subscriber_name)
        except Exception as e:  # pylint: disable=broad-exception-caught
            self._log.fatal(
                "Subscriber %s could not be unsubscribed (unknown error: %s).",
                self.subscriber_name, e,
                exc_info=e)

    async def send(self, payload: str):
        """Send a custom message.

        Args:
            payload (str):  Message payload to send.
        """
        self._log.debug("Sending message: %s", payload)
        await self._is_connected.wait()
        await self._connection.send(payload)
        self._log.debug("Message sent: %s", payload)

    async def ack(self, msg_id: str = None, payload: str = None):
        """Acknowledge a Notification 2.0 message.

        Either a valid Notification 2.0 message ID or payload needs to be
        provided. The message ID is extracted from the payload if necessary
        and sends it to the channel to signal the message handling
        completeness.

        Args:
            msg_id (str): Message ID to acknowledge.
            payload (str):  Raw Notification 2.0 message payload.

        See also:
            - Function `Message.ack` to acknowledge a specific Notification
              2.0 message directly.
            - `Listener` parameter `auto_ack=True` to automatically
              acknowledge a processed message
        """
        msg_id = msg_id or payload.splitlines()[0]
        await self.send(msg_id)


class Listener(object):
    """Synchronous (blocking) Notification 2.0 listener.

    Notification 2.0 events are distributed via Pulsar topics, communicating
    via websockets.

    This class encapsulates the Notification 2.0 communication protocol,
    providing a standard callback mechanism.

    Note: Listening with callback requires some sort of parallelism. This
    listener is implemented in a blocking fashion, it therefore requires
    the use of treads or subprocesses to ensure the parallelism.
    Class `AsyncListener` implements the same functionality using a
    non-blocking asynchronous approach.

    See also: https://cumulocity.com/guides/reference/notifications/
    """

    _log = logging.getLogger(__name__ + '.Listener')

    class Message(_Message):
        """Represents a Notification 2.0 message.

        This class is intended to be used with class `Listener` only.
        """

        def __init__(self, listener: "Listener", payload: str):
            """Create a new Notification 2.0 message.

            Args:
                listener (Listener):  Reference to the originating listener
                payload (str):  Raw message payload
            """
            super().__init__(payload)
            self.listener = listener

        def ack(self):
            """Acknowledge the message."""
            self.listener.ack(self.id)

    def __init__(
            self,
            c8y: CumulocityApi,
            subscription_name: str,
            subscriber_name: str = None,
            consumer_name: str = None,
            shared: bool = False,
            auto_ack: bool = True,
            auto_unsubscribe: bool = True,
    ):
        """Create a new Listener.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            subscription_name (str):  Subscription name
            subscriber_name (str): Subscriber (consumer) name; a sensible default
                is used when this is not defined.
        """
        # actual attributes need to be set like this to allow getter/setter propagation
        self._listener = AsyncListener(
            c8y=c8y,
            subscription_name=subscription_name,
            subscriber_name=subscriber_name,
            consumer_name=consumer_name,
            shared=shared,
            auto_ack=auto_ack,
            auto_unsubscribe=auto_unsubscribe,
        )
        self._event_loop = None
        self._thread = None

    # Note: c8y is read-only as changing it post-init would be complex.
    @property
    def c8y(self) -> CumulocityApi:
        # pylint: disable=missing-function-docstring
        return self._listener.c8y

    @property
    def subscription_name(self) -> str:
        # pylint: disable=missing-function-docstring
        return self._listener.subscription_name

    @subscription_name.setter
    def subscription_name(self, value: str):
        self._listener.subscription_name = value

    @property
    def subscriber_name(self) -> str:
        # pylint: disable=missing-function-docstring
        return self._listener.subscriber_name

    @subscriber_name.setter
    def subscriber_name(self, value: str):
        self._listener.subscriber_name = value

    @property
    def consumer_name(self) -> str:
        # pylint: disable=missing-function-docstring
        return self._listener.consumer_name

    @consumer_name.setter
    def consumer_name(self, value: str):
        self._listener.consumer_name = value

    @property
    def shared(self) -> bool:
        # pylint: disable=missing-function-docstring
        return self._listener.shared

    @shared.setter
    def shared(self, value: bool):
        self._listener.shared = value

    @property
    def auto_ack(self) -> bool:
        # pylint: disable=missing-function-docstring
        return self._listener.auto_ack

    @auto_ack.setter
    def auto_ack(self, value: bool):
        self._listener.auto_ack = value

    @property
    def auto_unsubscribe(self) -> bool:
        # pylint: disable=missing-function-docstring
        return self._listener.auto_unsubscribe

    @auto_unsubscribe.setter
    def auto_unsubscribe(self, value: bool):
        self._listener.auto_unsubscribe = value

    @property
    def signed_token(self) -> bool:
        # pylint: disable=missing-function-docstring
        return self._listener.signed_token

    @signed_token.setter
    def signed_token(self, value: bool):
        self._listener.signed_token = value

    @property
    def token_validity(self) -> int:
        # pylint: disable=missing-function-docstring
        return self._listener.token_validity

    @token_validity.setter
    def token_validity(self, value: int):
        self._listener.token_validity = value

    @property
    def ping_interval(self) -> int:
        # pylint: disable=missing-function-docstring
        return self._listener.ping_interval

    @ping_interval.setter
    def ping_interval(self, value: int):
        self._listener.ping_interval = value

    @property
    def ping_timeout(self) -> int:
        # pylint: disable=missing-function-docstring
        return self._listener.ping_timeout

    @ping_timeout.setter
    def ping_timeout(self, value: int):
        self._listener.ping_timeout = value

    @property
    def retry_interval(self) -> float:
        # pylint: disable=missing-function-docstring
        return self._listener.retry_interval

    @retry_interval.setter
    def retry_interval(self, value: float):
        self._listener.retry_interval = value

    @property
    def retry_rate(self) -> float:
        # pylint: disable=missing-function-docstring
        return self._listener.retry_rate

    @retry_rate.setter
    def retry_rate(self, value: float):
        self._listener.retry_rate = value

    @property
    def retry_max_delay(self) -> int:
        # pylint: disable=missing-function-docstring
        return self._listener.retry_max_delay

    @retry_max_delay.setter
    def retry_max_delay(self, value: int):
        self._listener.retry_max_delay = value

    def listen(self, callback: Callable[["Message"], None]):
        """Listen and handle messages.

        This function starts listening for new Notification 2.0 messages on
        the websocket channel. Each received message is wrapped in a `Message`
        object and forwarded to the callback function for handling.

        The messages are not automatically acknowledged. This can be done
        via the `Message` object's `ack` function by the callback function.

        Note: the callback function is invoked as a task and not awaited.

        This function will automatically handle the websocket communication
        including the authentication via tokens and reconnecting on
        connection loss. It will end when the listener is closed using its
        `close` function.
        """
        # a simple sync-to-async callback wrapper; error capture is still
        # done by the AsyncListener class
        async def _callback(message: AsyncListener.Message):
            callback(Listener.Message(self, message.raw))

        self._log.info("Listener started ...")

        # we need to create an event loop to run the async listener in
        self._event_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._event_loop)
        try:
            self._event_loop.run_until_complete(self._listener.listen(_callback))
        finally:
            self._event_loop.close()
        self._log.info("Listener stopped.")

    def start(self, callback: Callable[["Message"], None]) -> threading.Thread:
        """Start the listener.

        This function will start the listening process (`listen` function)
        within a thread and register the callback function to be invoked
        on every subscribed notification.

        Args:
            callback: Function to be invoked on notifications

        Returns:
            The listener thread.
        """
        self._thread = threading.Thread(target=self.listen, args=(callback,))
        self._thread.start()
        return self._thread

    def stop(self):
        """Stop the listener."""
        self._event_loop.call_soon_threadsafe(self._listener.stop)

    def wait(self, timeout=None) -> bool:
        """Wait for the listener to stop.

        Args:
            timeout (float):  Timeout in seconds.

        Returns:
            Whether the listener has stopped (before timeout).
        """
        self._thread.join(timeout=timeout)
        return not self._thread.is_alive()

    def unsubscribe(self):
        """Unsubscribe the listener.

        Manually unsubscribing is required if the listener wasn't created
        with `auto_unsubscribe=True`.

        See also https://cumulocity.com/api/core/#section/Overview/Consumers-and-tokens
        """
        self._event_loop.call_soon_threadsafe(self._listener.unsubscribe)

    def send(self, payload: str):
        """Send a custom message.

        Args:
            payload (str):  Message payload to send.
        """
        asyncio.run_coroutine_threadsafe(self._listener.send(payload), self._event_loop)

    def ack(self, msg_id: str = None, payload: str = None) -> None:
        """Acknowledge a Notification 2.0 message.

        Either a valid Notification 2.0 message ID or payload needs to be
        provided. The message ID is extracted from the payload if necessary
        and sends it to the channel to signal the message handling
        completeness.

        Args:
            msg_id (str): Message ID to acknowledge.
            payload (str):  Raw Notification 2.0 message payload.

        See also:
            - Function `Message.ack` to acknowledge a specific Notification
              2.0 message directly.
            - `Listener` parameter `auto_ack=True` to automatically
              acknowledge a processed message
        """
        # assuming that we are already listening ...
        asyncio.run_coroutine_threadsafe(self._listener.ack(msg_id, payload), self._event_loop)


class AsyncQueueListener(object):
    """Special listener implementation which pushes notification messages
    into a standard (async) queue which can be monitored and read."""

    def __init__(
            self,
            c8y: CumulocityApi,
            subscription_name: str,
            subscriber_name: str = None,
            consumer_name: str = None,
            shared: bool = False,
            auto_unsubscribe: bool = True,
            queue: asyncio.Queue = None
    ):
        self.queue = queue or asyncio.Queue()
        self.listener = AsyncListener(
            c8y=c8y,
            subscription_name=subscription_name,
            subscriber_name=subscriber_name,
            consumer_name=consumer_name,
            shared=shared,
            auto_ack=True,
            auto_unsubscribe=auto_unsubscribe,
        )

    def start(self):
        """Start the listener."""
        async def push_message(msg: AsyncListener.Message):
            self.queue.put_nowait(msg)

        self.listener.start(push_message)

    def stop(self):
        """Stop the listener."""
        self.listener.stop()

    async def wait(self, timeout=None):
        """Wait for the listener task to finish.

        Args:
            timeout (int): The number of seconds to wait for the listener
                to finish. The listener will be cancelled if the timeout
                occurs.
        """
        await self.listener.wait(timeout=timeout)


class QueueListener(object):
    """Special listener implementation which pushes notification messages
    into a standard (sync) queue which can be monitored and read."""

    def __init__(
            self,
            c8y: CumulocityApi,
            subscription_name: str,
            subscriber_name: str = None,
            consumer_name: str = None,
            shared: bool = False,
            auto_unsubscribe: bool = True,
            queue: sync_queue.Queue = None
    ):
        self.queue = queue or sync_queue.Queue()
        self.listener = Listener(
            c8y=c8y,
            subscription_name=subscription_name,
            subscriber_name=subscriber_name,
            consumer_name=consumer_name,
            shared=shared,
            auto_ack=True,
            auto_unsubscribe=auto_unsubscribe,
        )

    def start(self):
        """Start the listener."""

        def push_message(msg: AsyncListener.Message):
            self.queue.put(msg)

        self.listener.start(push_message)

    def stop(self):
        """Stop the listener."""
        self.listener.stop()

    def wait(self, timeout=None) -> bool:
        """Wait for the listener task to finish.

        Args:
            timeout (int): The number of seconds to wait for the listener
                to finish. The listener will be cancelled if the timeout
                occurs.

        Returns:
            Whether the listener has stopped (before timeout).
        """
        return self.listener.wait(timeout=timeout)
