# Copyright (c) 2025 Cumulocity GmbH
# pylint: disable=too-many-lines

from __future__ import annotations

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model._base import SimpleObject
from c8y_api.model._parser import SimpleObjectParser

from c8y_api.model.managedobjects import ManagedObject


class ExternalId(SimpleObject):
    """ Represents an instance of an ExternalID in Cumulocity.

    Instances of this class are returned by functions of the corresponding
    Identity API. Use this class to create or remove external ID.

    See also: https://cumulocity.com/api/#tag/External-IDs
    """

    _parser = SimpleObjectParser({
        'type': 'type',
        'external_id': 'externalId'
    })

    def __init__(self, c8y: CumulocityRestApi = None, external_id: str = None, external_type: str = None,
                 managed_object_id: str = None):
        """ Create a new ExternalId object.

        Args:
            c8y (CumulocityRestApi): Cumulocity connection reference; needs
            to be set for direct manipulation (create, delete) to function.
            external_id (str):  A string to be used as ID for external use
            external_type (str):  Type of the external ID,
                e.g. `_com_cumulocity_model_idtype_SerialNumber_`
            managed_object_id (str): Valid database ID of a managed object
                within Cumulocity

        Returns:
            ExternalId object
        """
        super().__init__(c8y=c8y)
        self.external_id = external_id
        self.external_type = external_type
        self.managed_object_id = managed_object_id

    def __repr__(self):
        return self._repr('external_type', 'external_id', 'managed_object_id')

    @classmethod
    def from_json(cls, json: dict) -> ExternalId:
        """ Build a new ExternalId instance from JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Args:
            json (dict):  JSON structure of an external ID object

        Returns:
            ExternalId object
        """
        external_id: ExternalId = super()._from_json(json, ExternalId())
        external_id.managed_object_id = json['managedObject']['id']
        return external_id

    def to_json(self, only_updated: bool = False) -> dict:
        # no documentation update
        json = super()._to_json(False)
        json['managedObject'] = {'id': self.managed_object_id}
        return json

    def create(self) -> ExternalId:
        """ Store the external ID to the database.

        Returns:
            `self` reference
        """
        self._assert_c8y()
        Identity(self.c8y).create(self.external_id, self.external_type, self.managed_object_id)
        return self

    def delete(self, **_) -> None:
        """ Remove the external ID from the database.

        Returns:
            `self` reference
        """
        self._assert_c8y()
        Identity(self.c8y).delete(self.external_id, self.external_type)

    def get_id(self):
        """ Read the referenced managed object ID from database.

        Returns:
            Database ID referenced by the external_id and external_type
            of this instance.
        """
        self._assert_c8y()
        Identity(self.c8y).get_id(self.external_id, self.external_type)

    def get_object(self) -> ManagedObject:
        """ Read the referenced managed object from database.

        Returns:
            ManagedObject instance.
        """
        self._assert_c8y()
        return Identity(self.c8y).get_object(self.external_id, self.external_type)


class Identity(object):
    # the Identity API of C8Y uses inconsistent resource paths and therefore
    # cannot use the generic CumulocityResource base class helper
    """ Provides access to the Identity API.

    See also: https://cumulocity.com/api/#tag/External-IDs
              https://cumulocity.com/api/#tag/Identity-API
    """

    def __init__(self, c8y: CumulocityRestApi):
        self.c8y = c8y
        self._inventory_instance = None

    @property
    def _inventory(self):
        if not self._inventory_instance:
            mod = __import__('c8y_api.model.inventory')
            self._inventory_instance = mod.model.Inventory(self.c8y)
        return self._inventory_instance

    def create(self, external_id, external_type, managed_object_id):
        """ Create a new External ID within the database.

        Args:
            external_id (str):  A string to be used as ID for external use
            external_type (str):  Type of the external ID,
                e.g. `_com_cumulocity_model_idtype_SerialNumber_`
            managed_object_id (str): Valid database ID of a managed object
                within Cumulocity
        """
        body_json = {
            'externalId': external_id,
            'type': external_type}
        path = f'/identity/globalIds/{managed_object_id}/externalIds'
        self.c8y.post(path, body_json)

    def delete(self, external_id, external_type) -> None:
        """ Remove an External ID from the database.

        Args:
            external_id (str):  A string to be used as ID for external use
            external_type (str):  Type of the external ID,
                e.g. `_com_cumulocity_model_idtype_SerialNumber_`
        """
        self.c8y.delete(self._build_object_path(external_id, external_type))

    def get(self, external_id, external_type):
        """ Obtain a specific External ID from the database.

        Args:
            external_id (str):  A string to be used as ID for external use
            external_type (str):  Type of the external ID,
                e.g. `_com_cumulocity_model_idtype_SerialNumber_`

        Returns:
            ExternalID object
        """
        return ExternalId.from_json(self._get_raw(external_id, external_type))

    def get_id(self, external_id, external_type):
        """ Read the ID of the referenced managed object by its external ID.

        Args:
            external_id (str):  A string to be used as ID for external use
            external_type (str):  Type of the external ID,
                e.g. `_com_cumulocity_model_idtype_SerialNumber_`

        Returns:
            A database ID (string)
        """
        return self._get_raw(external_id, external_type)['managedObject']['id']

    def get_object(self, external_id, external_type):
        """ Read a managed object by its external ID reference.

        Args:
            external_id (str):  A string to be used as ID for external use
            external_type (str):  Type of the external ID,
                e.g. `_com_cumulocity_model_idtype_SerialNumber_`

        Returns:
            ManagedObject instance
        """
        return self._inventory.get(self.get_id(external_id, external_type))

    def get_all(self, object_id:str) -> [ExternalId]:
        """ Read all external ID for a managed object.

        Args:
            object_id (str):  Valid database ID of a managed object
                within Cumulocity

        Returns:
            A list of ExternalID instances (can be empty)
        """
        external_ids = self.c8y.get(f'/identity/globalIds/{object_id}/externalIds')
        return [ExternalId.from_json(i) for i in external_ids['externalIds']]

    def _get_raw(self, external_id, external_type):
        return self.c8y.get(self._build_object_path(external_id, external_type))

    @staticmethod
    def _build_object_path(external_id, external_type):
        return f'/identity/externalIds/{external_type}/{external_id}'
