# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=missing-function-docstring

import threading
import time

from c8y_api import ProcessingMode
from c8y_api.app import SimpleCumulocityApp
from c8y_api.model import ManagedObject, Subscription
from c8y_tk.notification2 import Listener

from util.testing_util import RandomNameGenerator, load_dotenv

# load environment from a .env if present
load_dotenv()

# initialize Cumulocity connection
c8y = SimpleCumulocityApp()  # PERSISTENT
c8y_transient = SimpleCumulocityApp(processing_mode=ProcessingMode.TRANSIENT)
c8y_quiescent = SimpleCumulocityApp(processing_mode=ProcessingMode.QUIESCENT)

print("CumulocityApp(s) initialized.")
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
listener = Listener(c8y, sub.name)

# Define callback function.
# This function is invoked (synchronously) for each received event.
def callback(msg: Listener.Message):
    print(f"Received message, ID: {msg.id}, Source: {msg.source}, Action: {msg.action}, Body: {msg.json}")
    msg.ack()

# Wrap listen function into separate thread
# and start listening
listener_thread = threading.Thread(target=listener.listen, args=[callback])
listener_thread.start()

# Some action: Update the managed object
time.sleep(5)
mo['cx_CustomFragment'] = {'num': 42}
print("Updated managed object (persistent).")
mo.update()

time.sleep(1)
mo_transient = c8y_transient.inventory.get(mo.id)
print(f"Managed Object fragments: {mo_transient.fragments}")
mo_transient['cx_Transient'] = {'num': 42}
print("Updated managed object (transient).")
mo_transient.update()

time.sleep(1)
mo_quiescent = c8y_quiescent.inventory.get(mo.id)
print(f"Managed Object fragments: {mo_quiescent.fragments}")
mo_quiescent['cx_Quiescent'] = {'num': 42}
print("Updated managed object (quiescent).")
mo_quiescent.update()

time.sleep(1)
no = c8y.inventory.get(mo.id)
print(f"Final Managed Object fragments: {mo.fragments}")


# The update event is now being processed
time.sleep(5)

# close the listener
listener.close()

# cleanup subscription and managed object
sub.delete()
mo.delete()
