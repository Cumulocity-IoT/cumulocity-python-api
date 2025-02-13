# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations


from dotenv import load_dotenv
from inputimeout import inputimeout, TimeoutOccurred

from c8y_api.app import SimpleCumulocityApp
from c8y_api.model import Celsius, Device, Measurement, Operation

# A simple (per tenant) Cumulocity application can be created just like this.
# The authentication information is read from the standard Cumulocity
# environment variables that are injected into the Docker container.

load_dotenv()  # load environment from a .env if present
c8y = SimpleCumulocityApp()
print("CumulocityApp initialized.")
print(f"{c8y.base_url}, Tenant: {c8y.tenant_id}, User:{c8y.username}")

c8y.get_user_instance()

# The SimpleCumulocityApp behaves just like any other CumulocityApi instance,
# e.g. ...

# Reading users:
print("\nRegistered users:")
for u in c8y.users.get_all():
    print(f"  {u.username}, {u.id}")

# Reading devices:
print("\nDevices:")
for d in c8y.device_inventory.get_all(page_size=100):
    print(f"  {d.name} #{d.id}")

# Creating devices
new_device = Device(c8y, type='test_SomeDevice', name='MyTestDevice', custom_fragment={'foo': 'bar'},
                    com_cumulocity_model_Agent={}).create()
print(f"\nCreated new device: {new_device.name} #{new_device.id}")

# Creating Measurements
print("\nMeasurements:")
for v in range(0, 10):
    m = Measurement(c8y, type='test_SomeMeasurementType', source=new_device.id,
                    c8y_TemperatureMeasurement={'t': Celsius(v)}).create()
    print(f"  Created measurement: #{m.id}, JSON: {m.to_full_json()}")

# Creating Operation
print("\nOperation")
new_operation = Operation(c8y, new_device.id, 'Shell command', c8y_Command={'text': 'myCommand'})
new_operation.create()

operationList = c8y.operations.get_all(agent_id=new_device.id, status='PENDING', page_size=1)
pending_operation = operationList[0]
print(pending_operation.status)

pending_operation.status = 'EXECUTING'
pending_operation.update()


# Cleaning up
print("\n\nCleanup:\n\n")

wait_time = 300  # seconds
try:
    inputimeout(f"Press ENTER to continue. (Timeout: {wait_time}s)", timeout=wait_time)
except TimeoutOccurred:
    pass

# Removing measurements
c8y.measurements.delete_by(source=new_device.id)
print('\nMeasurements removed.')

new_device.delete()
print('\nDevice removed.')
