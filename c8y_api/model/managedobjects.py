from __future__ import annotations

from datetime import datetime
from typing import Any

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model.administration import User, Users
from c8y_api.model._base import _DictWrapper, SimpleObject, ComplexObject
from c8y_api.model._parser import ComplexObjectParser
from c8y_api.model._util import _DateUtil


class NamedObject(object):
    """ Represent a named object within the database.

    This class is used to model Cumulocity references.
    """
    def __init__(self, id=None, name=None):  # noqa
        """ Create a new instance.

        :param id:  Database ID of the object
        :param name:  Name of the object
        :returns:  New NamedObject instance
        """
        self.id = id
        self.name = name

    @classmethod
    def from_json(cls, object_json):
        """ Build a new instance from JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        :param object_json:  JSON object (nested dictionary)
            representing a named object within Cumulocity
        :returns:  NamedObject instance
        """
        return NamedObject(id=object_json['id'], name=object_json.get('name', ''))

    def to_json(self):
        """ Convert the instance to JSON.

        The JSON format produced by this function is what is used by the
        Cumulocity REST API.

        :returns:  JSON object (nested dictionary)
        """
        return {'id': self.id, 'name': self.name}


class Fragment(object):
    """ Represent a custom fragment within database objects.

    This class is used by other classes to represent custom fragments
    within their data model.

    For example, every measurement contains such a fragment, holding
    the actual data points:

        "pt_current": {
            "CURR": {
                "unit": "A",
                "value": 50
            }
        }

    A fragment has a name (*pt_current* in above example) and can virtually
    define any substructure.
    """
    def __init__(self, name: str, **kwargs):
        """ Create a new fragment.

        Params
            name (str):  Name of the fragment
            kwargs:  Named elements of the fragment. Each element
                can either be a simple value of a complex substructure
                modelled as nested dictionary.
        Returns:
            New Fragment instance
        """
        self.name = name
        self.items = kwargs

    def __getattr__(self, name: str):
        """ Get a specific element of the fragment.

        Args:
            name (str):  Name of the element

        Returns:
            Value of the element. May be a simple value or a
            complex substructure defined as nested dictionary.
        """
        item = self.items[name]
        return item if not isinstance(item, dict) else _DictWrapper(item)

    def has(self, name: str) -> bool:
        """ Check whether a specific element is defined.

        Args:
            name (str):  Name of the element

        Returns:
            True if the element is present, False otherwise
        """
        return name in self.items

    def add_element(self, name, element):
        """ Add an element.

        :param name:  Name of the element
        :param element:  Value of the element, either a simple value or
            a complex substructure defined as nested dictionary.
        :returns:  self
        """
        self.items[name] = element
        return self


class Availability(object):
    """Cumulocity availability status labels"""

    class ConnectionStatus:
        """Connection status labels"""
        CONNECTED = 'CONNECTED'
        DISCONNECTED = 'DISCONNECTED'

    class DataStatus:
        """Data status labels"""
        AVAILABLE = 'AVAILABLE'
        UNAVAILABLE = 'UNAVAILABLE'

    def __init__(self):
        self.connection_status = None
        self.data_status = None
        self.device_id = None
        self.external_id = None
        self.interval = None
        self.last_message = None

    @property
    def last_message_date(self) -> datetime:
        """Return the last message date as Python datetime object."""
        return _DateUtil.to_datetime(self.last_message)

    @property
    def interval_minutes(self) -> int:
        """Return the required update interval in minutes as integer."""
        return int(self.interval.split(' ', 1)[0])

    @classmethod
    def from_json(cls, object_json: dict) -> Availability:
        """Create an object instance from Cumulocity JSON format.

        Caveat: this function is primarily for internal use and does not
        return a full representation of the JSON. It is used for object
        creation and update within Cumulocity.

        Args:
            object_json (dict): The JSON to parse.

        Returns:
            A Availability instance.
        """
        obj = Availability()
        obj.device_id = object_json['deviceId']
        obj.external_id = object_json['externalId']
        obj.connection_status = object_json['connectionStatus']
        obj.data_status = object_json['dataStatus']
        obj.interval = object_json['interval']
        obj.last_message = object_json['lastMessage']
        return obj


class ManagedObjectUtil:
    """Utility functions to work with the Inventory API."""

    @staticmethod
    def build_managed_object_reference(object_id: str | int) -> dict:
        """Build the JSON for a managed object reference.

        Args:
            object_id (str|int): ID of a managed object

        Returns
            JSON (nested dict) for a managed object reference.
        """
        return {'managedObject': {'id': str(object_id)}}


class ManagedObject(ComplexObject):
    """ Represent a managed object within the database.

    Instances of this class are returned by functions of the corresponding
    Inventory API. Use this class to create new or update managed objects.

    Within Cumulocity a managed object is used to hold virtually any
    *additional* (apart from measurements, events and alarms) information.
    This custom information is modelled in *fragments*, named elements
    of any structure.

    Fragments are modelled as standard Python fields and can be accessed
    directly if the names & structures are known:

        x = mo.c8y_CustomFragment.values.x

    Managed objects can be changed and such updates are written as
    *differences* to the database. The API does the tracking of these
    differences automatically - just use the ManagedObject class like
    any other Python class.

        mo.owner = 'admin@cumulocity.com'
        mo.c8y_CustomFragment.region = 'EMEA'
        mo.add_fragment('c8y_CustomValue', value=12, uom='units')

    Note: This does not work if a fragment is actually a field, not a
    structure own its own. A direct assignment to such a value fragment,
    like

        mo.c8y_CustomReferences = [1, 2, 3]

    is currently not supported nicely as it will not be recognised as an
    update. A manual update flagging is required:

        mo.c8y_CustomReferences = [1, 2, 3]
        mo.flag_update('c8y_CustomReferences')

    See also https://cumulocity.com/guides/reference/inventory/#managed-object
    """

    _resource = '/inventory/managedObjects'
    _accept = CumulocityRestApi.ACCEPT_MANAGED_OBJECT

    _not_updatable = {'creation_time', 'update_time'}
    _parser = ComplexObjectParser(
        {'_u_type': 'type',
         '_u_name': 'name',
         '_u_owner': 'owner',
         'creation_time': 'creationTime',
         'update_time': 'lastUpdated'},
        ['childDevices', 'childAssets', 'childAdditions',
         'deviceParents', 'assetParents', 'additionParents'])

    class Resource:
        """Standard resource names."""
        AVAILABILITY = 'availability'
        SUPPORTED_MEASUREMENTS = 'supportedMeasurements'
        SUPPORTED_SERIES = 'supportedSeries'

    class Fragment:
        """Standard fragment names."""
        SUPPORTED_MEASUREMENTS = 'c8y_SupportedMeasurements'
        SUPPORTED_SERIES = 'c8y_SupportedSeries'


    def __init__(self, c8y: CumulocityRestApi = None,
                 type: str = None, name: str = None, owner: str = None, **kwargs):  # noqa
        """ Create a new ManagedObject instance.

        Custom fragments can be added to the object using `kwargs` or after
        creation using += or [] syntax.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            type (str):  ManagedObject type
            name (str):  ManagedObject name
            owner (str):  User ID of the owning user (can be left None to
                automatically assign to the connection user upon creation)
            kwargs:  Additional arguments are treated as custom fragments

        Returns:
            ManagedObject instance
        """
        super().__init__(c8y, **kwargs)
        # a direct update to the property backends is necessary to bypass
        # the update notification; everything specified within the constructor is
        # not considered to be an update
        self._u_type = type
        self._u_name = name
        self._u_owner = owner
        self.creation_time = None
        self.update_time = None
        self.child_devices = []
        self.child_assets = []
        """List of NamedObject references to child assets."""
        self.child_additions = []
        self.parent_devices = []
        self.parent_assets = []
        self.parent_additions = []
        self._object_path = None
        self.is_device = False
        self.is_device_group = False
        self.is_binary = False

    type = SimpleObject.UpdatableProperty(name='_u_type')
    name = SimpleObject.UpdatableProperty(name='_u_name')
    owner = SimpleObject.UpdatableProperty(name='_u_owner')

    @property
    def creation_datetime(self):
        """ Convert the object's creation to a Python datetime object.

        Returns:
            Standard Python datetime object
        """
        return super()._to_datetime(self.creation_time)

    @property
    def update_datetime(self):
        """ Convert the object's creation to a Python datetime object.

        Returns:
            Standard Python datetime object
        """
        return super()._to_datetime(self.update_time)

    @classmethod
    def _from_json(cls, json: dict, obj: Any) -> Any:
        # pylint: disable=arguments-differ
        """This function is used by derived classes to share the logic.
        Purposely no type information."""
        mo = super(ManagedObject, cls)._from_json(json, obj)
        if 'c8y_IsDevice' in json:
            mo.is_device = True
        if 'c8y_IsBinary' in json:
            mo.is_binary = True
        if 'childDevices' in json:
            mo.child_devices = cls._parse_references(json['childDevices'])
        if 'childAssets' in json:
            mo.child_assets = cls._parse_references(json['childAssets'])
        if 'childAdditions' in json:
            mo.child_additions = cls._parse_references(json['childAdditions'])
        return mo

    @classmethod
    def from_json(cls, json: dict) -> ManagedObject:
        """ Build a new ManagedObject instance from JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Args:
            json (dict): JSON object (nested dictionary)
                representing a managed object within Cumulocity

        Returns:
            ManagedObject object
        """
        return cls._from_json(json, ManagedObject())

    def to_json(self, only_updated=False) -> dict:
        json = super().to_json(only_updated)
        if not only_updated:
            if self.is_device:
                json['c8y_IsDevice'] = {}
            if self.is_device_group:
                json['c8y_IsDeviceGroup'] = {}
            if self.is_binary:
                json['c8y_IsBinary'] = ''
        return json

    @classmethod
    def _parse_references(cls, base_json):
        return [NamedObject.from_json(j['managedObject']) for j in base_json['references']]

    def create(self) -> ManagedObject:
        """ Create a new representation of this object within the database.

        This function can be called multiple times to create multiple
        instances of this object with different ID.

        Returns:
            A fresh ManagedObject instance representing the created
            object within the database. This instance can be used to get
            at the ID of the new managed object.

        See also function Inventory.create which doesn't parse the result.
        """
        return self._create()

    def update(self) -> ManagedObject:
        """ Write changes to the database.

        Returns:
            A fresh ManagedObject instance representing the updated
            object within the database.

        See also function Inventory.update which doesn't parse the result.
        """
        return self._update()

    def apply_to(self, other_id: str | int) -> ManagedObject:
        """Apply the details of this object to another object in the database.

        Note: This will take the full details, not just the updates.

        Args:
            other_id (str|int):  Database ID of the event to update.
        Returns:
            A fresh ManagedObject instance representing the updated
            object within the database.

        See also function Inventory.apply_to which doesn't parse the result.
        """
        self._assert_c8y()
        # put diff json to another object (by ID)
        result_json = self.c8y.put(self.__RESOURCE + str(other_id), json=self.to_diff_json(),
                                   accept=self.c8y.ACCEPT_MANAGED_OBJECT)
        result = ManagedObject.from_json(result_json)
        result.c8y = self.c8y
        return result

    def delete(self):
        """ Delete this object within the database.

        The database ID must be defined for this to function.

        See also function Inventory.delete to delete multiple objects.
        """
        self._delete()

    def add_child_asset(self, child: ManagedObject | str | int):
        """ Link a child asset to this managed object.

        This operation is executed immediately. No additional call to
        the `update` method is required.

        Args:
            child (ManagedObject|str|int): Child asset or its object ID
        """
        self._add_any_child('/childAssets', child)

    def add_child_device(self, child: ManagedObject | str | int):
        """ Link a child device to this managed object.

        This operation is executed immediately. No additional call to
        the `update` method is required.

        Args:
            child (ManagedObject|str|int): Child device or its object ID
        """
        self._add_any_child('/childDevices', child)

    def add_child_addition(self, child: ManagedObject | str | int):
        """ Link a child addition to this managed object.

        This operation is executed immediately. No additional call to
        the `update` method is required.

        Args:
            child (ManagedObject|str|int): Child addition or its object ID
        """
        self._add_any_child('/childAdditions', child)

    def unassign_child_asset(self, child: ManagedObject | str | int):
        """Remove the link to a child asset.

        This operation is executed immediately. No additional call to
        the `update` method is required.

        Args:
            child (ManagedObject|str|int): Child device or its object ID
        """
        self._unassign_any_child('/childAssets', child)

    def unassign_child_device(self, child: Device | str | int):
        """Remove the link to a child device.

        This operation is executed immediately. No additional call to
        the `update` method is required.

        Args:
            child (Device|str|int): Child device or its object ID
        """
        self._unassign_any_child('/childDevices', child)

    def unassign_child_addition(self, child: ManagedObject | str | int):
        """Remove the link to a child addition.

        This operation is executed immediately. No additional call to
        the `update` method is required.

        Args:
            child (ManagedObject|str|int): Child device or its object ID
        """
        self._unassign_any_child('/childAdditions', child)

    def _add_any_child(self, path, child: ManagedObject | str | int):
        self._assert_c8y()
        self._assert_id()
        child_id = child.id if hasattr(child, 'id') else child
        self.c8y.post(self._build_object_path() + path,
                      json=ManagedObjectUtil.build_managed_object_reference(child_id),
                      accept=None)

    def _unassign_any_child(self, path, child: ManagedObject | str | int):
        self._assert_c8y()
        self._assert_id()
        child_id = child.id if hasattr(child, 'id') else child
        self.c8y.delete(self._build_object_path() + path + '/' + child_id)

    def get_latest_availability(self) -> Availability:
        """Retrieve the latest availability information of this object.

        Return:
            DeviceAvailability object
        """
        self._assert_c8y()
        self._assert_id()
        result_json = self.c8y.get(self._build_object_path() + '/' + ManagedObject.Resource.AVAILABILITY)
        return Availability.from_json(result_json)

    def get_supported_measurements(self) -> datetime | str:
        """Retrieve all supported measurement names of this managed object.

        Return:
            List of measurement fragment names.
        """
        self._assert_c8y()
        self._assert_id()
        result_json = self.c8y.get(self._build_object_path() + '/' + self.Resource.SUPPORTED_MEASUREMENTS)
        return result_json[ManagedObject.Fragment.SUPPORTED_MEASUREMENTS]

    def get_supported_series(self) -> datetime | str:
        """Retrieve all supported measurement series names of this managed object.

        Return:
            List of measurement series names.
        """
        self._assert_c8y()
        self._assert_id()
        result_json = self.c8y.get(self._build_object_path() + '/' + self.Resource.SUPPORTED_SERIES)
        return result_json[ManagedObject.Fragment.SUPPORTED_SERIES]


class Device(ManagedObject):
    """ Represent an instance of a Device object within Cumulocity.

    Instances of this class are returned by functions of the corresponding
    DeviceInventory API. Use this class to create new or update Device
    objects.

    Device objects are regular managed objects with additional standardized
    fragments and fields.

    See also https://cumulocity.com/guides/reference/inventory/#managed-object
        https://cumulocity.com/guides/reference/device-management/
    """

    def __init__(self, c8y: CumulocityRestApi = None,
                 type: str = None, name: str = None, owner: str = None, **kwargs):  # noqa
        """ Create a new Device instance.

        A Device object will always have a `c8y_IsDevice` fragment.
        Additional custom fragments can be added using `kwargs` or
        after creation, using += or [] syntax.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            type (str):  Device type
            name (str):  Device name
            owner (str):  User ID of the owning user (can be left None to
                automatically assign to the connection user upon creation)
            kwargs:  Additional arguments are treated as custom fragments

        Returns:
            Device instance
        """
        super().__init__(c8y=c8y, type=type, name=name, owner=owner, **kwargs)
        self.is_device = True

    @classmethod
    def from_json(cls, json: dict) -> Device:
        # (no doc changes)
        return super()._from_json(json, Device())

    def to_json(self, only_updated=False) -> dict:
        # (no doc changes)
        object_json = super().to_json(only_updated)
        if not only_updated:
            object_json['c8y_IsDevice'] = {}
        return object_json

    def get_username(self) -> str:
        """Return the device username.

        Returns:
            Username of the device's user.
        """
        assert self.name, "Device name must be defined."
        return 'device_' + self.name

    def get_user(self) -> User:
        """Return the device user.

        Returns:
            Device's user.
        """
        return Users(self.c8y).get(self.get_username())


class DeviceGroup(ManagedObject):
    """ Represent a device group within Cumulocity.

    Instances of this class are returned by functions of the corresponding
    DeviceGroupInventory API. Use this class to create new or update
    DeviceGroup objects.

    DeviceGroup objects are regular managed objects with additional
    standardized fragments and fields.

    See also https://cumulocity.com/guides/reference/inventory/#managed-object
        https://cumulocity.com/guides/users-guide/device-management/#grouping-devices
    """

    ROOT_TYPE = 'c8y_DeviceGroup'
    CHILD_TYPE = 'c8y_DeviceSubGroup'

    def __init__(self, c8y=None, root: bool = False, name: str = None, owner: str = None, **kwargs):
        """ Build a new DeviceGroup object.

        A type of a device group will always be either `c8y_DeviceGroup`
        or `c8y_DeviceSubGroup` (depending on it's level). This is handled
        by the API.

        A DeviceGroup object will always have a `c8y_IsDeviceGroup` fragment.
        Additional custom fragments can be added using `kwargs` or after
        creation, using += or [] syntax.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            root (bool):  Whether the group is a root group (default is False)
            name (str):  Device name
            owner (str):  User ID of the owning user (can be left None to
                automatically assign to the connection user upon creation)
            kwargs:  Additional arguments are treated as custom fragments

        Returns:
            DeviceGroup instance
        """
        super().__init__(c8y=c8y, type=self.ROOT_TYPE if root else self.CHILD_TYPE,
                         name=name, owner=owner, **kwargs)
        self._added_child_groups = None
        self.is_device_group = True

    @classmethod
    def from_json(cls, json: dict) -> DeviceGroup:
        """ Build a new DeviceGroup instance from JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Args:
            json (dict): JSON object (nested dictionary)
                representing a device group within Cumulocity

        Returns:
            DeviceGroup instance
        """
        return super()._from_json(json, DeviceGroup())

    def create_child(self, name: str, owner: str = None, **kwargs) -> DeviceGroup:
        """ Create and assign a child group.

        This change is written to the database immediately.

        Args:
            name (str):  Device name
            owner (str):  User ID of the owning user (can be left None to
                automatically assign to the connection user upon creation)
            kwargs:  Additional arguments are treated as custom fragments

        Returns:
            The newly created DeviceGroup object
        """
        self._assert_c8y()
        self._assert_id()
        child = DeviceGroup(c8y=self.c8y, name=name, owner=owner if owner else self.owner, **kwargs).create()
        self.c8y.post(self._build_object_path() + '/childAssets', json={'managedObject': {'id': child.id}})
        return child

    def create(self) -> DeviceGroup:
        """ Create a new representation of this object within the database.

        This operation will create the group and all added child groups
        within the database.

        :returns:  A fresh DeviceGroup instance representing the created
            object within the database. This instance can be used to get at
            the ID of the new object.

        See also function DeviceGroupInventory.create which doesn't parse
        the result.
        """
        return super()._create()

    def update(self) -> DeviceGroup:
        """ Write changed to the database.

        Note: Removing child groups is currently not supported.

        :returns:  A fresh DeviceGroup instance representing the updated
            object within the database.
        """
        return super()._update()

    def delete(self):
        """Delete this device group.

        The child groups (if there are any) are left dangling. This is
        equivalent to using the `cascade=false` parameter in the
        Cumulocity REST API.
        """
        self._assert_c8y()
        self._assert_id()
        self.c8y.delete(self._build_object_path() + '?cascade=false')

    def delete_tree(self):
        """Delete this device group and its children.

        This is equivalent to using the `cascade=true` parameter in the
        Cumulocity REST API.
        """
        self._assert_c8y()
        self._assert_id()
        self.c8y.delete(self._build_object_path() + '?cascade=true')

    def assign_child_group(self, child: DeviceGroup | str | int):
        """Link a child group to this device group.

        This operation is executed immediately. No additional call to
        the `update` method is required.

        Args:
            child (DeviceGroup|str|int): Child device or its object ID
        """
        super().add_child_asset(child)

    def unassign_child_group(self, child: DeviceGroup | str | int):
        """Remove the link to a child group.

        This operation is executed immediately. No additional call to
        the `update` method is required.

        Args:
            child (DeviceGroup|str|int): Child device or its object ID
        """
        super().unassign_child_asset(child)
