# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=redefined-outer-name

import os
import threading
import time
from datetime import datetime

import pytest

from c8y_api import CumulocityApi, CumulocityDeviceRegistry
from c8y_api.model import Device
from util.testing_util import RandomNameGenerator


@pytest.fixture(scope='session')
def device_registry(test_environment, logger) -> CumulocityDeviceRegistry:
    """Provide a device registry instance."""

    # the live_c8y instance already read/updated the environment
    try:
        base_url = os.environ['C8Y_BASEURL']
        bootstrap_tenant = os.environ['C8Y_DEVICEBOOTSTRAP_TENANT']
        bootstrap_user = os.environ['C8Y_DEVICEBOOTSTRAP_USER']
        bootstrap_password = os.environ['C8Y_DEVICEBOOTSTRAP_PASSWORD']
    except KeyError as e:
        raise RuntimeError(f"Missing Cumulocity environment variable: {e} "
                           "Please define the required variables directly or setup a .env file.") from e

    return CumulocityDeviceRegistry(base_url, bootstrap_tenant, bootstrap_user, bootstrap_password)


@pytest.fixture(scope='function')
def sample_device(live_c8y: CumulocityApi, device_registry: CumulocityDeviceRegistry, logger) -> Device:
    """Provide a sample device, created via the device registry process."""

    device_id = RandomNameGenerator.random_name(3)

    # 1) create a device connection request
    live_c8y.device_inventory.request(device_id)

    # 2) continuously try to accept the request
    # It can be accepted once there was some communication
    # we will do this asynchronously
    def await_communication_and_accept():
        # pylint: disable=bare-except
        for _ in range(1, 100):
            try:
                live_c8y.device_inventory.accept(device_id)
                break
            except:
                logger.info("Unable to accept device request. Waiting for device communication.")
                time.sleep(0.5)
    threading.Thread(target=await_communication_and_accept).start()

    # 3) Wait for the request acceptance
    logger.info(f"Requesting credentials for device '{device_id}'.")
    device_api = device_registry.await_connection(device_id)
    logger.info("Credentials request accepted.")

    # 4) Create a digital twin
    device = Device(c8y=device_api, name=device_id, type='c8y_TestDevice').create()
    logger.info(f"Device created: '{device_id}', ID: {device.id}, Owner:{device.owner}")

    yield device

    logger.info("Deleting the device (and user) ...")

    device.delete()
    logger.info(f"Device '{device_id}' deleted.")
    live_c8y.users.delete(device.owner)
    logger.info(f"User '{device.owner}' deleted.")


def test_device_created(sample_device: Device):
    """Verify that the sample device was created properly."""

    # -> should have a database ID
    assert sample_device.id

    # -> should have been created less that 10s before
    now = time.time()
    creation_time = datetime.timestamp(sample_device.creation_datetime)
    assert creation_time - now < 10

    # -> should have a proper device user as owner
    assert sample_device.owner == sample_device.c8y.username
    assert sample_device.owner == sample_device.get_username()
