# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

# pylint: disable=redefined-outer-name

import logging
import os
from typing import List, Callable

import pytest
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

from c8y_api._main_api import CumulocityApi
from c8y_api._util import c8y_keys
from c8y_api.app import SimpleCumulocityApp
from c8y_api.model import Application, Device
from util.testing_util import RandomNameGenerator


@pytest.fixture(scope='session')
def safe_executor(logger):
    """A safe function execution wrapper.

    This provides a `execute(fun)` function which catches/logs all
    exceptions. It returns True if the wrapped function was executed
    without error, False otherwise.
    """
    # pylint: disable=broad-except

    def execute(fun) -> bool:
        try:
            fun()
            return True
        except BaseException as e:
            logger.warning(f"Caught exception ignored due to safe call: {e}")
        return False

    return execute


@pytest.fixture(scope='session')
def logger():
    """Provide a logger for testing."""
    return logging.getLogger('c8y_api.test')


@pytest.fixture(scope='session')
def test_environment(logger):
    """Prepare the environment, i.e. read a .env file if found."""

    # check if there is a .env file
    if os.path.exists('.env'):
        logger.info("Environment file (.env) exists and will be considered.")
        # check if any C8Y_ variable is already defined
        predefined_keys = c8y_keys()
        if predefined_keys:
            logger.fatal("The following environment variables are already defined and may be overridden: "
                         + ', '.join(predefined_keys))
        load_dotenv()
    # list C8Y_* keys
    defined_keys = c8y_keys()
    logger.info(f"Found the following keys: {', '.join(defined_keys)}.")


@pytest.fixture(scope='session')
def live_c8y(test_environment) -> CumulocityApi:
    """Provide a live CumulocityApi instance as defined by the environment."""
    if 'C8Y_BASEURL' not in os.environ:
        raise RuntimeError("Missing Cumulocity environment variables (C8Y_*). Cannot create CumulocityApi instance. "
                           "Please define the required variables directly or setup a .env file.")
    return SimpleCumulocityApp()


@pytest.fixture(scope='session')
def app_factory(live_c8y) -> Callable[[str, List[str]], CumulocityApi]:
    """Provide a application (microservice) factory which creates a
    microservice application within Cumulocity, registers itself as
    subscribed tenant and returns the application's bootstrap user.

    All created microservice applications are removed after the tests.
    The factory users must ensure the uniqueness of the application
    names within the entire test session.

    Args:
        live_c8y:  (injected) connection to a live Cumulocity instance; the
            user must be allowed to create microservice applications.

    Returns:
        A factory function with two arguments, application name (string) and
        application roles (list of strings).
    """
    created: List[Application] = []

    def factory_fun(name: str, roles: List[str]):

        # (1) Verify this application is not registered, yet
        if live_c8y.applications.get_all(name=name):
            raise ValueError(f"Microservice application named '{name}' seems to be already registered.")

        # (2) Create application stub in Cumulocity
        settings = [{
                "defaultValue": "",
                "key": x,
            } for x in ("keyA", "keyB")]
        app = Application(live_c8y, name=name, key=f'{name}-key',
                          type=Application.MICROSERVICE_TYPE,
                          availability=Application.PRIVATE_AVAILABILITY,
                          manifest={"settings": settings},
                          required_roles=roles).create()
        created.append(app)

        # (3) Subscribe to newly created microservice
        subscription_json = {'application': {'self': f'{live_c8y.base_url}/application/applications/{app.id}'}}
        live_c8y.post(f'/tenant/tenants/{live_c8y.tenant_id}/applications', json=subscription_json)
        print(f"Microservice application '{name}' (ID {app.id}) created. Tenant '{live_c8y.tenant_id}' subscribed.")

        # (4) read bootstrap user details
        bootstrap_user_json = live_c8y.get(f'/application/applications/{app.id}/bootstrapUser')

        # (5) create bootstrap instance
        bootstrap_c8y = CumulocityApi(base_url=live_c8y.base_url,
                                      tenant_id=bootstrap_user_json['tenant'],
                                      auth=HTTPBasicAuth(bootstrap_user_json['name'], bootstrap_user_json['password']))
        print(f"Bootstrap instance created.  Tenant {bootstrap_c8y.tenant_id}, "
              f"User: {bootstrap_c8y.auth.username}, "
              f"Password: {bootstrap_c8y.auth.password}")

        return bootstrap_c8y

    yield factory_fun

    # unregister application
    for a in created:
        live_c8y.applications.delete(a.id)
        print(f"Microservice application '{a.name}' (ID {a.id}) deleted.")


@pytest.fixture(scope='session')
def factory(logger, live_c8y: CumulocityApi):
    """Provides a generic object factory function which ensures that created
    objects are removed after testing."""

    created = []

    def factory_fun(obj):
        if not obj.c8y:
            obj.c8y = live_c8y
        o = obj.create()
        logger.info(f"Created object #{o.id}, ({o.__class__.__name__})")
        created.append(o)
        return o

    yield factory_fun

    for c in created:
        c.delete()
        logger.info(f"Removed object #{c.id}, ({c.__class__.__name__})")


@pytest.fixture(scope='session')
def sample_device(logger: logging.Logger, live_c8y: CumulocityApi) -> Device:
    """Provide an sample device, just for testing purposes."""

    typename = RandomNameGenerator.random_name()
    device = Device(live_c8y, type=typename, name=typename, com_cumulocity_model_Agent={}).create()
    logger.info(f"Created test device #{device.id}, name={device.name}")

    yield device

    device.delete()
    logger.info(f"Deleted test device #{device.id}")
