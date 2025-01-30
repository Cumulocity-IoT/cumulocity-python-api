# Copyright (c) 2025 Cumulocity GmbH
# pylint: disable=broad-except, missing-class-docstring, missing-function-docstring

import logging
import uuid
import threading
import time

import dotenv

from c8y_api import CumulocityDeviceRegistry, CumulocityApi, UnauthorizedError
from c8y_api.model import Device, Event, ManagedObject
from c8y_api.model._util import _DateUtil


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
c8y.device_requests.request(DEVICE_ID)
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


# --- Testing auto-acknowlede of device requests

# 1) Device stores random security token in platform
security_token = str(uuid.uuid4())
security_asset = ManagedObject(c8y=device_c8y, name=device.name, type="c8y_SecurityToken",
                               c8y_SecurityToken={'token': security_token, 'since': _DateUtil.now_timestring()},
                               ).create()
device.add_child_addition(security_asset)
logger.info(f"Security token: {security_token}, Stored as object #{security_asset.id}")

# 2) Create an Auto Ack thread
class AutoAckThread(threading.Thread):
    def __init__(self, max_time: float = 60):
        super().__init__()
        self.max_time = max_time
        self._stop_event = threading.Event()

    def stop(self):
        self._stop_event.set()

    def run(self):
        # this thread continuously selects new device requests and automatically
        # tries to acknowledge them when it finds matching security token (same name)
        # within the platform
        logger.info("Starting AutoAck thread ...")
        start_time = time.monotonic()
        while not self._stop_event.is_set():
            for device_id, status in c8y.device_requests.select():
                logger.info(f"Found device request; ID: {device_id}, Status: {status}")
                for mo in c8y.inventory.select(name=device_id, type='c8y_SecurityToken'):
                    logger.info(f"Found matching security token asset: #{mo.id}")
                    token = mo.c8y_SecurityToken.token
                    try:
                        c8y.device_requests.accept(device_id, security_token=token)
                        logger.info("Device request auto-accepted.")
                        continue
                    except UnauthorizedError as error:
                        logger.error(f"Auto accept failed: {str(error)}")
            if time.monotonic() > (start_time + self.max_time):
                logger.info("Max wait time reached. Stopping ...")
            time.sleep(5)
            logger.info("Waiting ...")
        logger.info("AutoAck thread stopped.")

ack_thread = AutoAckThread(60)
ack_thread.start()

# 3) Assume that the device credentials got lost, but we simply request
#    again, using the stored security token as shared secret
logger.info("Requesting device credentials (again) ...")
c8y.device_requests.request(DEVICE_ID)
try:
    device_c8y = registry.await_connection(DEVICE_ID, security_token=security_token, timeout='5h', pause='5s')
except Exception as e:
    logger.error("Got error", exc_info=e)

ack_thread.stop()
ack_thread.join()

# 4) Update the digital twin using the new connection
device_c8y.inventory.apply_to({'c8y_AdditionalFragment': {'some': 'data'}}, device.id)

# 5) Send an event using new connection
event = Event(c8y=device_c8y, type='c8y_TestEvent', time='now',
              source=device.id, text="Test event").create()

# 6) Verify changes to managed object
device = c8y.device_inventory.get(device.id)
assert device.c8y_AdditionalFragment.some == 'data'
assert len(c8y.events.get_all(source=device.id)) == 2

# --- Cleanup ------------------------------------------------------------

device.delete()
logger.info(f"Device '{DEVICE_ID}' deleted.")
c8y.users.delete(device.owner)
logger.info(f"User '{device.owner}' deleted.")
