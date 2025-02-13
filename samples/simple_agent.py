# Copyright (c) 2025 Cumulocity GmbH

import os
import uuid

import dotenv

from c8y_api import CumulocityDeviceRegistry
from c8y_api.model import Device, Event

# Usually, each Cumulocity device agent has its own access credentials which
# are created by Cumulocity during the device registration process.
# This sample simulates this using multiple threads and Cumulocity connections.
#
# See also: https://cumulocity.com/guides/users-guide/device-management/#connecting-devices
#
# The authentication information is read from the environment. Please provide
# the environment variables mentioned below (a .env file is accepted as well).

dotenv.load_dotenv()
base_url = os.environ['C8Y_BASEURL']
tenant_id = 'management'
bootstrap_username = os.environ['C8Y_DEVICEBOOTSTRAP_USER']
bootstrap_password = os.environ['C8Y_DEVICEBOOTSTRAP_PASSWORD']

device_serial = f'c8y_api-{uuid.uuid1()}'

print(f"Generated device serial: {device_serial}"
      "\nPlease open the Cumulocity UI and register a device for this serial.")
input("\nPress ENTER to continue.")

# The device registry is a special version of the Cumulocity API,
# it should be used using device bootstrap credentials.
c8y_registry = CumulocityDeviceRegistry(base_url, tenant_id, bootstrap_username, bootstrap_password)

print("\nThis client will now continuously query for the device credentials."
      "\nPlease approve the request within the Cumulocity UI.")
# The registry provides two auxiliary functions, `await_credentials` which
# blocks until the device registration was acknowledged and `await_connection`
# which does the same and automatically constructs a corresponding connection
c8y_device = c8y_registry.await_connection(device_serial)
print(f"Device registration successful. Username: {c8y_device.username}")

# The device connection is then used to define the device's digital twin
# within the Cumulocity database:
device = Device(c8y_device, type='sag_PythonDevice', name='Sample Python Device').create()
print(f"Digital twin created. Database ID: {device.id}")

# It is recommended to register the external ID as a serial as well:
c8y_device.identity.create(device_serial, 'c8y_Serial', device.id)
print("External ID created.")

# We now send a simple event from the device
event = Event(c8y_device, type='sag_PythonInitDone', source=device.id, time='now',
              text='Device initialization done.').create()

# Cleaning up
input("\nPress ENTER to continue to cleanup.")

print("\nCleanup:\n")

# Removing the external ID
c8y_device.identity.delete(device_serial, 'c8y_Serial')
print("External ID removed.")

# Removing the device
c8y_device.device_inventory.delete(device)  # this will also remove the device user
print("Digital twin removed (including user).")
