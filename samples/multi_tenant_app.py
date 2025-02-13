# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

import logging

from dotenv import load_dotenv
from flask import Flask, request, jsonify
from http.client import HTTPConnection

from c8y_api._base_api import UnauthorizedError
from c8y_api.app import MultiTenantCumulocityApp


# A multi-tenant aware Cumulocity application can be created just like this.
# The bootstrap authentication information is read from the standard
# Cumulocity environment variables that are injected into the Docker
# container.
# The MultiTenantCumulocityApp class is not a CumulocityApi instance (in
# contrast to SimpleCumulocityApp), it acts as a factory to provide
# specific CumulocityApi instances for subscribed tenants  and users.

# load environment from a .env if present
load_dotenv()

# enable full logging for requests
HTTPConnection.debuglevel = 1
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True

# initialize cumulocity
c8yapp = MultiTenantCumulocityApp()
logging.info("CumulocityApp initialized.")
c8y_bootstrap = c8yapp.bootstrap_instance
logging.info(f"Bootstrap: {c8y_bootstrap.base_url}, Tenant: {c8y_bootstrap.tenant_id}, User:{c8y_bootstrap.username}")


# setup Flask
webapp = Flask(__name__)


@webapp.route("/health")
def health():
    """Return dummy health string."""
    return jsonify({'status': 'ok'})


@webapp.route("/debug")
def debug():
    """Return debug information."""
    return jsonify({
        'headers': dict(request.headers),
        'cookies': dict(request.cookies),
    })


@webapp.route("/tenant")
def tenant_info():
    """Return subscribed tenant's ID, username and devices it has access to."""
    # The subscribed tenant's credentials (to access Cumulocity and to access
    # the microservice) are part of the inbound request's headers. This is
    # resolved automatically when using the get_tenant_instance function.
    c8y = c8yapp.get_tenant_instance(headers=request.headers, cookies=request.cookies)
    logging.info(f"Obtained tenant instance: tenant: {c8y.tenant_id}, user: {c8y.username}, pass: {c8y.auth.password}")
    # If the tenant ID is known (e.g. from URL) it can be given directly
    # like this:
    # c8y = c8yapp.get_tenant_instance(tenant_id='t12345')
    tenant_json = {'tenant_id': c8y.tenant_id,
                   'base_url': c8y.base_url,
                   'username': c8y.username}
    devices_json = [{'name': d.name,
                     'id': d.id,
                     'type': d.type} for d in c8y.device_inventory.get_all()]
    info_json = {'tenant': tenant_json,
                 'devices': devices_json}
    return jsonify(info_json)


@webapp.route("/user")
def user_info():
    """Return user's tenant, username and devices they have access to."""
    # The user's credentials (to access Cumulocity and to access the
    # microservice) are part of the inbound request's headers. This is
    # resolved automatically when using the get_user_instance function.
    # Note: the user connections are cached, hence it can be possible to
    # receive an outdated, no longer valid connection. The corresponding
    # UnauthorizedError must be caught and dealt with.
    for _ in range(2):
        c8y = c8yapp.get_user_instance(request.headers, request.cookies)
        try:
            logging.info(f"Obtained user instance: tenant: {c8y.tenant_id}, user: {c8y.username}")
            devices_json = [{'name': d.name,
                             'id': d.id,
                             'type': d.type} for d in c8y.device_inventory.get_all()]
            info_json = {'username': c8y.username,
                         'devices': devices_json}
            return jsonify(info_json)
        except UnauthorizedError:
            c8yapp.clear_user_cache(c8y.username)
    raise RuntimeError("Unable to obtain a valid user scope connection!")


webapp.run(host='0.0.0.0', port=80)
