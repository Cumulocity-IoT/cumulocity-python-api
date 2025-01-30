# Copyright (c) 2025 Cumulocity GmbH

from typing import Generator, Tuple

from c8y_api.model._base import CumulocityObject

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model._base import CumulocityResource


class DeviceRequests(CumulocityResource):
    """Provides access to the Device Requests API.

    This class can be used to get, search for, create, update and
    delete device requests within the Cumulocity database.

    See also:  https://cumulocity.com/api/core/#tag/New-device-requests
    """
    class Status:
        """Device requests status strings."""
        WAITING_FOR_CONNECTION = 'WAITING_FOR_CONNECTION'
        PENDING_ACCEPTANCE = 'PENDING_ACCEPTANCE'
        ACCEPTED = 'ACCEPTED'

    class _DeviceRequest(CumulocityObject):
        """Internal class to enable standard functionality."""

        def __init__(self, c8y: CumulocityRestApi = None, id: str = None, status: str = None ):
            super().__init__(c8y)
            self.id = id
            self.status = status

        @classmethod
        def from_json(cls, json: dict) -> "DeviceRequests._DeviceRequest":
            """Create an object instance from Cumulocity JSON format.

            Caveat: this function is primarily for internal use and does not
            return a full representation of the JSON. It is used for object
            creation and update within Cumulocity.

            Args:
                json (dict): The JSON to parse.

            Returns:
                A DeviceRequests._DeviceRequest instance.
            """
            # pylint: disable=protected-access
            return DeviceRequests._DeviceRequest(id=json['id'], status=json['status'])


    def __init__(self, c8y):
        super().__init__(c8y, 'devicecontrol/newDeviceRequests')

    def select(
            self,
            limit: int = None,
            page_size: int = 100,
            page_number: int = None,
    ) -> Generator[Tuple[str, str], None, None]:
        """ Query the database for device requests and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        Args:
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of objects which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.
        Returns:
            Generator for tuples (device ID (str), status (str))
        """
        base_query = super()._prepare_query(
            resource='/devicecontrol/newDeviceRequests/',
            page_size=page_size,
        )
        for item in super()._iterate(base_query=base_query, page_number=page_number, limit=limit,
                                     parse_fun=DeviceRequests._DeviceRequest.from_json):
            yield item.id, item.status

    def request(self, id: str):  # noqa (id)
        """ Create a device request.

        Args:
            id (str): Unique ID of the device (e.g. Serial, IMEI); this is
            _not_ the database ID.
        """
        self.c8y.post(self.resource, {'id': id})

    def accept(self, id: str, security_token: str = None):  # noqa (id)
        """ Accept a device request.

        Args:
            id (str): Unique ID of the device (e.g. Serial, IMEI); this is
                _not_ the database ID.
            security_token (str):  Shared secret that is used to authorize
                the credentials request.
        """
        data = {'status': 'ACCEPTED'}
        if security_token:
            data['securityToken'] = security_token
        self.c8y.put(self.build_object_path(id), data)
