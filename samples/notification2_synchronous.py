# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=missing-function-docstring

import time

from c8y_api.app import SimpleCumulocityApp
from c8y_api.model import ManagedObject, Subscription
from c8y_tk.notification2 import Listener

from util.testing_util import RandomNameGenerator, load_dotenv


# load environment from a .env if present
load_dotenv()

# initialize Cumulocity connection
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
listener = Listener(
    c8y,
    subscription_name=sub.name,
    auto_ack=False,
    auto_unsubscribe=False,
)

# Define callback function.
# This function is invoked (synchronously) for each received event.
def callback(msg: Listener.Message):
    print(f"Received message, ID: {msg.id}, Source: {msg.source}, Action: {msg.action}, Body: {msg.json}")
    msg.ack()

# Start listening, thread is handled automatically
listener.start(callback)

# Some action: Update the managed object
time.sleep(5)
mo['cx_CustomFragment'] = {'num': 42}
mo.update()

# The update event is now being processed
time.sleep(5)

# stop the listener and wait for completion
listener.stop()
listener.wait()

# cleanup subscription and managed object
sub.delete()
mo.delete()
