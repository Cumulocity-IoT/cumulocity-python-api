# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.
import os

from c8y_api import CumulocityRestApi


def test_authenticate(test_environment):
    """Verify that a valid token is obtained from the environment."""

    base_url = os.environ['C8Y_BASEURL']
    tenant_id = os.environ['C8Y_TENANT']
    username = os.environ['C8Y_USER']
    password = os.environ['C8Y_PASSWORD']

    token, xsrf = CumulocityRestApi.authenticate(
        base_url=base_url,
        tenant_id=tenant_id,
        username=username,
        password=password
    )

    assert token
    assert xsrf
