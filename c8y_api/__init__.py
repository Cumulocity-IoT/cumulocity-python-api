# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from pkg_resources import get_distribution, DistributionNotFound

from c8y_api._base_api import (
    ProcessingMode,
    CumulocityRestApi,
    HttpError,
    UnauthorizedError,
    MissingTfaError,
    AccessDeniedError,
)
from c8y_api._main_api import CumulocityApi
from c8y_api._registry_api import CumulocityDeviceRegistry
from c8y_api._auth import HTTPBasicAuth, HTTPBearerAuth

try:
    __version__ = get_distribution(__name__).version
except DistributionNotFound:
    pass

__all__ = [
    'CumulocityApi',
    'CumulocityRestApi',
    'CumulocityDeviceRegistry',
    'HTTPBasicAuth',
    'HTTPBearerAuth',
    'ProcessingMode',
    'HttpError',
    'UnauthorizedError',
    'MissingTfaError',
    'AccessDeniedError',
    ]
