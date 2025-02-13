# Copyright (c) 2025 Cumulocity GmbH

import json
import zipfile

from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

from c8y_api import CumulocityApi
from c8y_api.app import SimpleCumulocityApp
from c8y_api.model import Application

_DIST_DIR = 'dist'


def format_sample_name(name: str) -> str:
    """Format a name as sample name (underscores instead of hyphens)."""
    return name.replace('-', '_')


def format_application_name(name: str) -> str:
    """Format a name as application name (hyphens instead of underscores)."""
    return name.replace('_', '-')


def register_microservice(sample_name: str):
    """ Register a microservice at Cumulocity.

    The Cumulocity connection information is taken from environment files
    (.env and .env-SAMPLE-NAME) located in the working directory.

    Args:
        sample_name (str):  The name of the sample to use (file name
            without .py extension)

    """
    application_name = format_application_name(sample_name)
    load_dotenv()
    c8y = SimpleCumulocityApp()

    # Verify this application is not registered, yet
    if c8y.applications.get_all(name=application_name):
        raise ValueError(f"Microservice application named '{application_name}' seems to be already registered.")

    # Read prepared binary .zip, extract manifest and parse
    zip_path = f'{_DIST_DIR}/samples/{sample_name}/{application_name}.zip'
    with zipfile.ZipFile(zip_path, 'r') as zip_file:
        manifest_json = json.loads(zip_file.read('cumulocity.json'))

    # Create application stub in Cumulocity
    required_roles = manifest_json['requiredRoles']
    app = Application(c8y, name=application_name, key=f'{application_name}-key',
                      type=Application.MICROSERVICE_TYPE,
                      availability=Application.PRIVATE_AVAILABILITY,
                      required_roles=required_roles)
    app = app.create()

    # Subscribe to newly created microservice
    subscription_json = {'application': {'self': f'{c8y.base_url}/application/applications/{app.id}'}}
    c8y.post(f'/tenant/tenants/{c8y.tenant_id}/applications', json=subscription_json)

    print(f"Microservice application '{application_name}' (ID {app.id}) created. Tenant '{c8y.tenant_id}' subscribed.")


def unregister_microservice(sample_name: str):
    """ Unregister a microservice from Cumulocity.

    The Cumulocity connection information is taken from environment files
    (.env and .env-SAMPLE-NAME) located in the working directory.

    Args:
        sample_name (str):  The name of the sample to use (file name
            without .py extension)

    Throws:
        LookupError  if a corresponding application cannot be found.
    """
    application_name = format_application_name(sample_name)
    load_dotenv()

    try:
        c8y = SimpleCumulocityApp()
        # read applications by name, will throw IndexError if there is none
        app = c8y.applications.get_all(name=application_name)[0]
        # delete by ID
        app.delete()
    except IndexError as e:
        raise LookupError(f"Cannot retrieve information for an application named '{application_name}'.") from e

    print(f"Microservice application '{application_name}' (ID {app.id}) deleted.")


def get_credentials(sample_name: str) -> (str, str):
    """ Get the bootstrap user credentials of a registered microservice.

    The Cumulocity connection information is taken from environment files
    (.env and .env-SAMPLE-NAME) located in the working directory.

    Args:
        sample_name (str):  The name of the sample to use (file name
            without .py extension)

    Returns:
        A pair (username, password) for the credentials.

    Throws:
        LookupError  if a corresponding application cannot be found.
    """
    application_name = format_application_name(sample_name)
    load_dotenv()

    c8y = SimpleCumulocityApp()
    try:
        # read applications by name, will throw IndexError if there is none
        app = c8y.applications.get_all(name=application_name)[0]
    except IndexError as e:
        raise LookupError(f"Cannot retrieve information for an application named '{application_name}'.") from e

    # read bootstrap user details
    bootstrap_user_json = c8y.get(f'/application/applications/{app.id}/bootstrapUser')
    # create bootstrap instance
    bootstrap_c8y = CumulocityApi(base_url=c8y.base_url,
                                  tenant_id=bootstrap_user_json['tenant'],
                                  auth=HTTPBasicAuth(bootstrap_user_json['name'], bootstrap_user_json['password']))
    # read all subscribed tenants, print first
    users_json = bootstrap_c8y.get('/application/currentApplication/subscriptions')['users']
    if not users_json:  # empty users element?
        raise LookupError(f"Cannot retrieve subscribed tenants for application named '{application_name}'.")

    return users_json[0]['tenant'], users_json[0]['name'], users_json[0]['password']
