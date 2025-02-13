# Copyright (c) 2025 Cumulocity GmbH

try:
    from importlib.metadata import version, PackageNotFoundError
except ModuleNotFoundError:
    from importlib_metadata import version, PackageNotFoundError

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
    __version__ = version(__name__)
except PackageNotFoundError:
    __version__ = 'LATEST'
