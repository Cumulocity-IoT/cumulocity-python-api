# Copyright (c) 2025 Cumulocity GmbH
# pylint: disable=broad-except

import logging
import dotenv

from c8y_api import CumulocityDeviceRegistry, CumulocityApi
from c8y_api.model import Device, Event


DEVICE_ID = 'BengalBonobo18'

# load environment from a .env file
env = dotenv.dotenv_values()
C8Y_BASEURL = env['C8Y_BASEURL']
C8Y_TENANT = env['C8Y_TENANT']
C8Y_USER = env['C8Y_USER']
C8Y_PASSWORD = env['C8Y_PASSWORD']
C8Y_DEVICEBOOTSTRAP_TENANT = env['C8Y_DEVICEBOOTSTRAP_TENANT']
C8Y_DEVICEBOOTSTRAP_USER = env['C8Y_DEVICEBOOTSTRAP_USER']
C8Y_DEVICEBOOTSTRAP_PASSWORD = env['C8Y_DEVICEBOOTSTRAP_PASSWORD']


logger = logging.getLogger('com.cumulocity.test.device_registry')
logging.basicConfig()
logger.setLevel('INFO')

# a regular Cumulocity connection to create/approve device requests and such
c8y = CumulocityApi(base_url=C8Y_BASEURL,
                    tenant_id=C8Y_TENANT,
                    username=C8Y_USER,
                    password=C8Y_PASSWORD)
# a special Cumulocity 'device registry' connection to get device credentials
registry = CumulocityDeviceRegistry(base_url=C8Y_BASEURL,
                                    tenant_id=C8Y_DEVICEBOOTSTRAP_TENANT,
                                    username=C8Y_DEVICEBOOTSTRAP_USER,
                                    password=C8Y_DEVICEBOOTSTRAP_PASSWORD)

# 1) create device request
c8y.device_inventory.request(DEVICE_ID)
logger.info(f"Device '{DEVICE_ID}' requested. Approve in Cumulocity now.")

# 2) await device credentials (approval within Cumulocity)
device_c8y = None
try:
    device_c8y = registry.await_connection(DEVICE_ID, timeout='5h', pause='5s')
except Exception as e:
    logger.error("Got error", exc_info=e)

# 3) Create a digital twin
device = Device(c8y=device_c8y, name=DEVICE_ID, type='c8y_TestDevice',
                c8y_RequiredAvailability={"responseInterval": 10}).create()
logger.info(f"Device created: '{device.name}', ID: {device.id}, Owner:{device.owner}")

# 4) send an event
event = Event(c8y=device_c8y, type='c8y_TestEvent', time='now',
              source=device.id, text="Test event").create()

# 5) check device's availability status
try:
    availability = c8y.get(f'/inventory/managedObjects/{device.id}/availability')
    logger.info(f"Device availability: {availability}")
except KeyError:
    logger.error("Device availability not defined!")

# 6) cleanup device
device.delete()
logger.info(f"Device '{DEVICE_ID}' deleted.")
c8y.users.delete(device.owner)
logger.info(f"User '{device.owner}' deleted.")
