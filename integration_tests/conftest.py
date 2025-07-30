# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=redefined-outer-name

import logging
import os
import sys
from typing import List, Callable, Any, Generator

import pytest
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

from c8y_api._main_api import CumulocityApi
from c8y_api._util import c8y_keys
from c8y_api.app import SimpleCumulocityApp
from c8y_api.model import Application, Device, ManagedObject

from util.testing_util import RandomNameGenerator


# Configure logging
logging.getLogger('urllib3').setLevel(logging.DEBUG)
logging.getLogger('websockets').setLevel(logging.DEBUG)
logging.getLogger('c8y_tk').setLevel(logging.DEBUG)
logging.getLogger('c8y_api').setLevel(logging.DEBUG)


@pytest.fixture(scope='function')
def random_name():
    """Conveniently provide a random name."""
    return RandomNameGenerator().random_name()


@pytest.fixture(scope='session')
def logger():
    """Provide a logger for testing."""
    handler = logging.StreamHandler(sys.__stderr__)
    logger = logging.getLogger('c8y_api.test')
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


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


@pytest.fixture(scope='function')
def auto_delete(logger):
    """Register a created Cumulocity object for automatic deletion after test function execution."""

    objects = []

    def register(obj) -> Any:
        objects.append(obj)

    yield register

    for o in objects:
        try:
            # Deletion should through a KeyError if object was already deleted
            o.delete()
        except KeyError:
            pass
        except BaseException as e:
            logger.warning(f"Caught exception ignored due to safe call: {e}")


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





#
#
# @pytest.fixture(scope='session')
# def register_object(logger):
#     """Wrap a created Cumulocity object so that it will automatically be deleted
#     after a test regardless of an exception or failure."""
#
#     objects = []
#
#     def register(obj) -> Any:
#         objects.append(obj)
#         return obj
#
#     yield register
#
#     for o in objects:
#         try:
#             # Deletion should through a KeyError if object was already deleted
#             o.delete()
#             logger.warning(f"Object #{o.id} was not deleted by test.")
#         except KeyError:
#             pass
#         except BaseException as e:
#             logger.warning(f"Caught exception ignored due to safe call: {e}")


@pytest.fixture(scope='function')
def safe_create(logger, live_c8y, request):
    """Wrap a created Cumulocity object so that it will automatically be deleted
    after a test regardless of an exception or failure.

    Deletion is still expected by the test, so this will log a warning if the
    object was not deleted and needed to be cleaned up."""
    objects_with_node = []

    def create_and_register(obj) -> Any:
        if not obj.c8y:
            obj.c8y = live_c8y
        o = obj.create()
        objects_with_node.append((o, request.node.name))
        return o

    yield create_and_register

    for o, node in objects_with_node:
        try:
            # Deletion should through a KeyError if object was already deleted
            o.delete()
            logger.warning(f"{type(o).__name__} object #{o.id} was not deleted by test '{node}'.")
        except KeyError:
            pass
        except BaseException as e:
            logger.error(f"Caught exception ignored due to safe call: {e} (node: {node})")


@pytest.fixture(scope='module')
def module_factory(logger, live_c8y: CumulocityApi, request):
    """Provides a generic object factory function which ensures that created
    objects are removed after the module testing.

    Deletion is _not_ expected by the test code."""

    created = []

    def factory_fun(obj):
        if not obj.c8y:
            obj.c8y = live_c8y
        o = obj.create()
        node = request.module.__name__
        logger.info(f"Created {o.__class__.__name__} object #{o.id} in module {node}.")
        created.append((o, node))
        return o

    yield factory_fun

    for o, node in created:
        try:
            o.delete()
            logger.info(f"Removed {o.__class__.__name__} #{o.id} from module {node}.")
        except KeyError:
            logger.warning(f"{o.__class__.__name__} object #{o.id} (module {node}) could not be removed (not found).")


@pytest.fixture(scope='session')
def app_factory(logger, live_c8y) -> Generator[Callable[[str, List[str]], CumulocityApi], None, None]:
    """Provide a application (microservice) factory which creates a
    microservice application within Cumulocity, registers itself as
    subscribed tenant and returns the application's bootstrap user.

    All created microservice applications are removed after the tests.
    The factory users must ensure the uniqueness of the application
    names within the entire test session.

    Args:
        logger:  (injected) test logger.
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
        logger.info(f"Microservice application '{name}' (ID {app.id}) created. "
                    f"Tenant '{live_c8y.tenant_id}' subscribed.")

        # (4) read bootstrap user details
        bootstrap_user_json = live_c8y.get(f'/application/applications/{app.id}/bootstrapUser')

        # (5) create bootstrap instance
        bootstrap_c8y = CumulocityApi(base_url=live_c8y.base_url,
                                      tenant_id=bootstrap_user_json['tenant'],
                                      auth=HTTPBasicAuth(bootstrap_user_json['name'], bootstrap_user_json['password']))
        logger.info(f"Bootstrap instance created.  Tenant {bootstrap_c8y.tenant_id}, "
              f"User: {bootstrap_c8y.auth.username}, "
              f"Password: {bootstrap_c8y.auth.password}")

        return bootstrap_c8y

    yield factory_fun

    # unregister application
    for a in created:
        try:
            live_c8y.applications.delete(a.id)
            logger.info(f"Microservice application '{a.name}' (ID {a.id}) deleted.")
        except KeyError:
            logger.warning(f"Application #{a.id} could not be removed (not found).")


@pytest.fixture(scope='function')
def sample_object(logger, live_c8y, random_name, auto_delete):
    """Provide a sample object which is automatically removed after test."""
    obj = ManagedObject(live_c8y, name=random_name, type=random_name).create()
    auto_delete(obj)
    return obj


@pytest.fixture(scope='session')
def session_device(logger: logging.Logger, live_c8y: CumulocityApi):
    """Provide an sample device, just for testing purposes."""

    typename = RandomNameGenerator.random_name()
    device = Device(live_c8y, type=typename, name=typename, com_cumulocity_model_Agent={}).create()
    logger.info(f"Created test device #{device.id}, name={device.name}")

    yield device

    device.delete()
    logger.info(f"Deleted test device #{device.id}")
