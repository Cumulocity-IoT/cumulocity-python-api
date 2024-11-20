# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

import asyncio
import json as js
import logging
from typing import Callable, Awaitable
import websockets as ws

from c8y_api import CumulocityApi


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

    def __init__(self, c8y: CumulocityApi, subscription_name: str):
        self.c8y = c8y
        self.subscription_name = subscription_name

        self._event_loop = None
        self._outbound_queue = []
        self._current_uri = None
        self._is_closed = False
        self._connection = None

    async def _get_connection(self) -> ws.WebSocketClientProtocol:
        if not self._connection:
            if not self._current_uri:
                token = self.c8y.notification2_tokens.generate(self.subscription_name, expires=2)
                self._current_uri = self.c8y.notification2_tokens.build_websocket_uri(token)
                self._log.debug("New Notification 2.0 token requested for subscription '{}'.", self.subscription_name)
            try:
                self._connection = await ws.connect(self._current_uri,
                                                    ping_interval=AsyncListener.ping_interval,
                                                    ping_timeout=AsyncListener.ping_timeout)
                self._log.info("Websocket connection established for subscription: {}", self.subscription_name)
            except ws.ConnectionClosed as e:
                self._log.info("Cannot open websocket connection. Closed: {}", e)
                self._connection = None
                self._current_uri = None  # maybe the URI can be reused if not expired?

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
                self._log.debug("Received message: {}.", payload)
                await asyncio.create_task(_callback(AsyncListener.Message(listener=self, payload=payload)))
            except ws.ConnectionClosed as e:
                self._log.info("Websocket connection closed: {}", e)

    async def send(self, payload: str):
        """Send a custom message.

        Args:
            payload (str):  Message payload to send.
        """
        websocket = await self._get_connection()
        self._log.debug("Sending message: {}", payload)
        await websocket.send(payload)
        self._log.debug("Message sent: {}", payload)

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
        self._log.debug("Message received: {}", payload)
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

    def __init__(self, c8y: CumulocityApi, subscription_name: str):
        self._listener = AsyncListener(c8y=c8y, subscription_name=subscription_name)
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
