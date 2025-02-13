# Copyright (c) 2025 Cumulocity GmbH

from c8y_api.app import SimpleCumulocityApp

from util.testing_util import load_dotenv

load_dotenv()
c8y = SimpleCumulocityApp()
print("CumulocityApp initialized.")
print(f"{c8y.base_url}, Tenant: {c8y.tenant_id}, User:{c8y.username}")

try:
    value1 = c8y.tenant_options.get_value(category='remoteaccess', key='credentials.encryption.password')
    print(f"Value: {value1}")
except KeyError:
    print("Unable to read encrypted tenant option.")
