# Copyright (c) 2025 Cumulocity GmbH

"""
This sample code demonstrates how to obtain Cumulocity user sessions (sessions
that are run within the context of a named user).

When writing a microservice for Cumulocity you always have two options to
get access to Cumulocity:

  a) Use a technical user' context. This is injected into the microservice
     via environment variables that the c8y_api automatically deals with.

  b) Use the context of whatever user accesses the microservice. The
     credentials for this context must be extracted from the inbound request.

The SimpleCumulocityApp and MultiTenantCumulocityApp classes can be used to
get a user specific CumulocityApi instance using the get_user_instance
function as illustrated below. This function will automatically extract the
authorization information within the inbound request's headers and build a
CumulocityApi instance based on that.
"""

from dotenv import load_dotenv
from flask import Flask, request, jsonify

from c8y_api.app import SimpleCumulocityApp

load_dotenv()
app = Flask(__name__)
c8y = SimpleCumulocityApp()


@app.route("/info")
def info():
    """Return user's username and devices they have access to."""
    # The user's credentials (to access Cumulocity and to access the
    # microservice) are part of the inbound request's headers. This is
    # resolved automatically when using the get_user_instance function.
    user_c8y = c8y.get_user_instance(request.headers)
    devices_json = [{'name': d.name,
                     'id': d.id,
                     'type': d.type} for d in user_c8y.device_inventory.get_all()]
    info_json = {'username': user_c8y.username,
                 'devices': devices_json}
    return jsonify(info_json)


app.run()
