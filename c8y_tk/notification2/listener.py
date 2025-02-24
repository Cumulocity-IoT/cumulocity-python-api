# Copyright (c) 2025 Cumulocity GmbH

import asyncio
import json as js
import logging
import time
from typing import Callable, Awaitable

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

    _log = logging.getLogger(__name__ + '.AsyncListener')
    ping_interval = 60
    ping_timeout = 20

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
            await self.listener.send(self.id)

    def __init__(self, c8y: CumulocityApi, subscription_name: str, subscriber_name: str = None):
        """Create a new Listener.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            subscription_name (str):  Subscription name
            subscriber_name (str): Subscriber (consumer) name; a sensible default
                is used when this is not defined.
        """
        self.c8y = c8y
        self.subscription_name = subscription_name
        self.subscriber_name = subscriber_name

        self._event_loop = None
        self._outbound_queue = []
        self._current_validity = None
        self._current_uri = None
        self._is_closed = False
        self._connection = None

    # Note: Return type naming differs for different Python Versions; ClientConnection
    # refers to the latest module revision
    async def _get_connection(self) -> ws_client.ClientConnection:
        if not self._connection:
            if self._current_uri and self._current_validity < time.time():
                self._current_uri = None
            if not self._current_uri:
                token = self.c8y.notification2_tokens.generate(
                    subscription=self.subscription_name,
                    subscriber=self.subscriber_name)
                self._current_uri = self.c8y.notification2_tokens.build_websocket_uri(token)
                self._current_validity = float(JWT(token).get_claim('exp'))
                self._log.debug("New Notification 2.0 token requested for subscription %s, %s",
                                self.subscription_name,
                                self.subscriber_name or 'default')
            try:
                self._log.debug("Connecting ...")
                self._connection = await ws_client.connect(self._current_uri,
                                                    ping_interval=AsyncListener.ping_interval,
                                                    ping_timeout=AsyncListener.ping_timeout)
                self._log.info("Websocket connection established for subscription %s, %s",
                               self.subscription_name,
                               self.subscriber_name or 'default')
            except InvalidStatus as e:
                self._log.info("Cannot open websocket connection. Failed: {e}", exc_info=e)
                self._connection = None
                raise e
            except ConnectionClosed as e:
                self._log.info("Cannot open websocket connection. Closed: {e}", exc_info=e)
                self._connection = None
                raise e

        return self._connection

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

        Args:
            callback (Callable):  A coroutine to be invoked on every inbound
                message.
        """
        # this unnecessary wrap seems to be necessary to suppress a compiler warning
        async def _callback(msg):
            await callback(msg)

        while not self._is_closed:
            try:
                c = await self._get_connection()
                payload = await c.recv()
                self._log.debug("Received message: %s", payload)
                await asyncio.create_task(_callback(AsyncListener.Message(listener=self, payload=payload)))
            except InvalidStatus as e:
                self._log.info("Websocket connection failed: %s", e)
            except ConnectionClosed as e:
                self._log.info("Websocket connection closed: %s", e)

    async def send(self, payload: str):
        """Send a custom message.

        Args:
            payload (str):  Message payload to send.
        """
        websocket = await self._get_connection()
        self._log.debug("Sending message: %s", payload)
        await websocket.send(payload)
        self._log.debug("Message sent: %s", payload)

    async def ack(self, payload: str):
        """Acknowledge a Notification 2.0 message.

        This extracts the message ID from the payload and sends it to the
        channel to acknowledge the message handling completeness.

        Args:
            payload (str):  Raw Notification 2.0 message payload.
        """
        msg_id = payload.splitlines()[0]
        await self.send(msg_id)

    async def receive(self):
        """Read a message.

        This will wait for an inbound message on the communication channel
        and return it (raw).

        Returns:
             The raw payload of the next inbound message.
        """
        websocket = await self._get_connection()
        self._log.debug("Waiting for message ...")
        payload = await websocket.recv()
        self._log.debug("Message received: %s", payload)
        return payload

    async def close(self):
        """Close the websocket connection."""
        self._log.info("Closing websocket connection ...")
        self._is_closed = True
        c = await self._get_connection()
        await c.close()


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
            self.listener.send(self.id)

    def __init__(self, c8y: CumulocityApi, subscription_name: str, subscriber_name: str = None):
        """Create a new Listener.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            subscription_name (str):  Subscription name
            subscriber_name (str): Subscriber (consumer) name; a sensible default
                is used when this is not defined.
        """
        self._listener = AsyncListener(c8y=c8y, subscription_name=subscription_name, subscriber_name=subscriber_name)
        self._event_loop = asyncio.new_event_loop()
        self._current_uri = None
        self._is_closed = False
        self._connection = None

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

        Args:
            callback (Callable):  A coroutine to be invoked on every inbound
                message.
        """
        async def _callback(message: AsyncListener.Message):
            msg = Listener.Message(self, message.raw)
            callback(msg)

        self._log.debug("Listening ...")
        self._event_loop.run_until_complete(self._listener.listen(_callback))
        self._log.debug("Stopped listening.")

    def send(self, payload: str) -> None:
        """Send a custom message.

        Args:
            payload (str):  Message payload to send.
        """
        # assuming that we are already listening ...
        asyncio.run_coroutine_threadsafe(self._listener.send(payload), self._event_loop)

    def ack(self, payload: str) -> None:
        """Acknowledge a Notification 2.0 message.

        This extracts the message ID from the payload and sends it to the
        channel to acknowledge the message handling completeness.

        Args:
            payload (str):  Raw Notification 2.0 message payload.
        """
        # assuming that we are already listening ...
        asyncio.run_coroutine_threadsafe(self._listener.ack(payload), self._event_loop)

    def receive(self) -> str:
        """Read a message.

        This will wait for an inbound message on the communication channel
        and return it (raw).

        Returns:
             The raw payload of the next inbound message.
        """
        if not self._event_loop.is_running():
            return self._event_loop.run_until_complete(self._listener.receive())

        future = asyncio.run_coroutine_threadsafe(self._listener.receive(), self._event_loop)
        return future.result()

    def close(self):
        """Close the websocket connection."""
        if self._event_loop.is_running():
            asyncio.run_coroutine_threadsafe(self._listener.close(), self._event_loop)
        else:
            self._event_loop.run_until_complete(self._listener.close())
