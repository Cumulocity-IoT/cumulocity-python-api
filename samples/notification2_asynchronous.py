# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=missing-function-docstring

import asyncio

from c8y_api.app import SimpleCumulocityApp
from c8y_api.model import ManagedObject, Subscription
from c8y_tk.notification2 import AsyncListener

from util.testing_util import RandomNameGenerator, load_dotenv


async def main():

    c8y = SimpleCumulocityApp()
    print("CumulocityApp initialized.")
    print(f"{c8y.base_url}, Tenant: {c8y.tenant_id}, User:{c8y.username}")

    # Create a managed object to play with
    mo_name = RandomNameGenerator.random_name(3)
    mo = ManagedObject(c8y, name=mo_name, type='c8y_CustomType').create()
    print(f"Managed object created: #{mo.id} '{mo_name}'")

    # Create a subscription to listen for updated on
    # previously created managed object
    sub_name = f'{mo_name.replace("_", "")}Subscription'
    sub = Subscription(c8y, name=sub_name, context=Subscription.Context.MANAGED_OBJECT, source_id=mo.id).create()
    print(f"Subscription created: {sub_name}")

    # Create a listener for previously created subscription
    listener = AsyncListener(c8y, sub.name)

    # Define callback function.
    # This function is invoked (asynchronously) for each received event.
    async def callback(msg:AsyncListener.Message):
        print(f"Received message, ID: {msg.id}, Source: {msg.source}, Action: {msg.action}, Body: {msg.json}")
        await msg.ack()

    # Start listening
    listener.start(callback)

    # Some action: Update the managed object
    await asyncio.sleep(5)
    mo['cx_CustomFragment'] = {'num': 42}
    mo.update()

    # The update event is now being processed
    await asyncio.sleep(5)

    # close the listener and wait for it to end.
    listener.stop()
    await listener.wait()

    # cleanup subscription and managed object
    sub.delete()
    mo.delete()


# load environment from a .env if present
load_dotenv()
# run main coroutine
asyncio.run(main())
