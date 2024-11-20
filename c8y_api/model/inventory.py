# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.
# pylint: disable=too-many-lines

from __future__ import annotations

from typing import Any, Generator, List

from c8y_api.model._base import CumulocityResource
from c8y_api.model._util import _QueryUtil
from c8y_api.model.managedobjects import ManagedObjectUtil, ManagedObject, Device, Availability, DeviceGroup


class Inventory(CumulocityResource):
    """Provides access to the Inventory API.

    This class can be used for get, search for, create, update and
    delete managed objects within the Cumulocity database.

    See also: https://cumulocity.com/api/#tag/Inventory-API
    """

    def __init__(self, c8y):
        super().__init__(c8y, 'inventory/managedObjects')

    def get(self, id) -> ManagedObject:  # noqa (id)
        """ Retrieve a specific managed object from the database.

        Args:
            ID of the managed object

        Returns:
             A ManagedObject instance

        Raises:
            KeyError:  if the ID is not defined within the database
        """
        managed_object = ManagedObject.from_json(self._get_object(id))
        managed_object.c8y = self.c8y  # inject c8y connection into instance
        return managed_object

    def get_all(
            self,
            expression: str = None,
            query: str = None,
            ids: list[str | int] = None,
            order_by: str = None,
            type: str = None,
            parent: str = None,
            fragment: str = None,
            fragments: list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            only_roots: str = None,
            with_children: bool = None,
            with_children_count: bool = None,
            skip_children_names: bool = None,
            with_groups: bool = None,
            with_parents: bool = None,
            with_latest_values: bool = None,
            reverse: bool = None,
            limit: int = None,
            page_size: int = 1000,
            **kwargs) -> List[ManagedObject]:
        """ Query the database for managed objects and return the results
        as list.

        This function is a greedy version of the `select` function. All
        available results are read immediately and returned as list.

        Returns:
            List of ManagedObject instances
        """
        return list(self.select(
            expression=expression,
            query=query,
            ids=ids,
            order_by=order_by,
            type=type,
            parent=parent,
            fragment=fragment,
            fragments=fragments,
            name=name,
            owner=owner,
            text=text,
            only_roots=only_roots,
            with_children=with_children,
            with_children_count=with_children_count,
            skip_children_names=skip_children_names,
            with_groups=with_groups,
            with_parents=with_parents,
            with_latest_values=with_latest_values,
            reverse=reverse,
            limit=limit,
            page_size=page_size,
            **kwargs))

    def get_count(
            self,
            expression: str = None,
            query: str = None,
            ids: List[str | int] = None,
            type: str = None,
            parent: str = None,
            fragment: str = None,
            fragments: list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            **kwargs) -> int:
        """Calculate the number of potential results of a database query.

        This function uses the same parameters as the `select` function.

        Returns:
            Number of potential results
        """
        base_query = self._prepare_inventory_query(
            device_mode=False,
            expression=expression,
            type=type,
            parent=parent,
            name=name,
            owner=owner,
            text=text,
            fragment=fragment,
            fragments=fragments,
            query=query,
            ids=ids,
            page_size=1,
            **kwargs)
        return self._get_count(base_query)

    def select(
            self,
            expression: str = None,
            query: str = None,
            ids: list[str | int] = None,
            order_by: str = None,
            type: str = None,
            parent: str = None,
            fragment: str = None,
            fragments: list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            only_roots: str = None,
            with_children: bool = None,
            with_children_count: bool = None,
            skip_children_names: bool = None,
            with_groups: bool = None,
            with_parents: bool = None,
            with_latest_values: bool = None,
            limit: int = None,
            page_size: int = 1000,
            page_number: int = None,
            **kwargs) -> Generator[ManagedObject]:
        """ Query the database for managed objects and iterate over the
        results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification.  Filters can be
        combined (within reason).

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            query (str):  Complex query to execute; all other filters are
                ignored if such a custom query is provided
            ids (List[str|int]): Specific object ID to select.
            order_by (str):  Field/expression to sort the results.
            type (str):  Managed object type
            parent (str):  Parent object in the asset hierarchy (ID).
            fragment (str):  Name of a present custom/standard fragment
            fragments (list[str]): Additional fragments present within objects
            name (str):  Name of the managed object
                Note: The Cumulocity REST API does not support filtering for
                names directly; this is a convenience parameter which will
                translate all filters into a query string.
            owner (str):  Username of the object owner
            text (str): Text value of any object property.
            only_roots (bool): Whether to include only objects that don't have
                any parent
            with_children (bool):  Whether children with ID and name should be
                included with each returned object
            with_children_count (bool): When set to true, the returned result
                will contain the total number of children in the respective
                child additions, assets and devices sub fragments.
            skip_children_names (bool):  If true, returned references of child
                devices won't contain their names.
            with_groups (bool): Whether to include additional information about
                the groups to which the searched managed object belongs to.
                This results in setting the assetParents property with
                additional information about the groups.
            with_parents (bool): Whether to include a device's parents.
            with_latest_values (bool):  If true the platform includes the
                fragment `c8y_LatestMeasurements, which contains the latest
                measurement values reported by the device to the platform.
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of events which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator for ManagedObject instances
        """

        return self._select(
            ManagedObject.from_json,
            device_mode=False,
            expression=expression,
            query=query,
            ids=ids,
            order_by=order_by,
            type=type,
            parent=parent,
            fragment=fragment,
            fragments=fragments,
            name=name,
            owner=owner,
            text=text,
            only_roots=only_roots,
            with_children=with_children,
            with_children_count=with_children_count,
            skip_children_names=skip_children_names,
            with_groups=with_groups,
            with_parents=with_parents,
            with_latest_values=with_latest_values,
            limit=limit,
            page_size=page_size,
            page_number=page_number,
            **kwargs)

    def _prepare_inventory_query(
            self,
            device_mode: bool,
            expression: str = None,
            query: str = None,
            ids: list[str | int] = None,
            filters: list[str] = None,
            order_by: str = None,
            type: str = None,
            parent: str = None,
            fragment: str = None,
            fragments: str | list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            **kwargs,
    ) -> str:
        if expression:
            return self._prepare_query(expression=expression)
        params = self._collate_filter_params(
            device_mode,
            query=query,
            ids=ids,
            filters=filters,
            order_by=order_by,
            type=type,
            parent=parent,
            fragment=fragment,
            fragments=fragments,
            name=name,
            owner=owner,
            text=text,
            **kwargs)
        return self._prepare_query(**params)

    @staticmethod
    def _collate_filter_params(
            only_devices: bool,
            # expression: str = None,
            query: str = None,
            ids: list[str | int] = None,
            filters: list[str] = None,
            order_by: str = None,
            type: str = None,
            parent: str = None,
            fragment: str = None,
            fragments: list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            **kwargs,
    ) -> dict:
        # if expression
        query_key = 'q' if only_devices else 'query'
        # if query is directly specified -> use it and ignore everything else
        if query:
            return {query_key: query, **kwargs}

        if ids:
            return {'ids': ids, **kwargs}

        def filter_none(**xs):
            return {k: v for k, v in xs.items() if v is not None}

        use_query = parent or filters or order_by or name or fragments
        if not use_query:
            return filter_none(type=type, owner=owner, text=text, fragment=fragment, **kwargs)

        # if any of the given filter is 'special' we have to convert to a query
        filters = filters or []

        # add fragment filters
        fragments = fragments or ([fragment] if fragment else [])
        if fragments:
            filters.extend([f'has({x})' for x in fragments])
        if parent:
            filters.append(f'bygroupid({parent})')
        if name:
            filters.append(f"name eq '{_QueryUtil.encode_odata_query_value(name)}'")
        if type:
            filters.append(f"type eq {type}")
        if owner:
            filters.append(f"owner eq {owner}")
        if text:
            filters.append(f"text eq '{_QueryUtil.encode_odata_query_value(text)}'")

        # convert to single query parameter
        order_by = f'+$orderby={order_by}' if order_by else ''
        query = f'$filter=({" and ".join(filters)}){order_by}'

        return {query_key: query, **kwargs}

    def _select(self, parse_fun, device_mode: bool, page_number, limit, **kwargs) -> Generator[Any]:
        """Generic select function to be used by derived classes as well."""
        base_query = self._prepare_inventory_query(device_mode, **kwargs)
        return super()._iterate(base_query, page_number, limit, parse_fun)

    def create(self, *objects: ManagedObject):
        """Create managed objects within the database.

        Args:
           *objects (ManagedObject): collection of ManagedObject instances
        """
        super()._create(ManagedObject.to_json, *objects)

    def update(self, *objects: ManagedObject):
        """Write changes to the database.

        Args:
           *objects (ManagedObject): collection of ManagedObject instances

        See also function `ManagedObject.update` which parses the result.
        """
        super()._update(ManagedObject.to_diff_json, *objects)

    def apply_to(self, object_model: ManagedObject | dict, *object_ids):
        """Apply a change to multiple already existing objects.

        Applies the details of a model object to a set of already existing
        managed objects.

        Note: This will take the full details, not just the updates.

        Args:
            object_model (ManagedObject|dict): ManagedObject instance holding
                the change structure (e.g. a specific fragment) or simply a
                dictionary representing the diff JSON.
            *object_ids (str): a collection of ID of already existing
                managed objects within the database
        """
        super()._apply_to(ManagedObject.to_full_json, object_model, *object_ids)

    def get_latest_availability(self, mo_id) -> Availability:
        """Retrieve the latest availability information of a managed object.

        Args:
            mo_id (str):  Device (managed object) ID

        Return:
            DeviceAvailability object
        """
        result_json = self.c8y.get(self.build_object_path(mo_id) + '/' + ManagedObject.Resource.AVAILABILITY)
        return Availability.from_json(result_json)

    def get_supported_measurements(self, mo_id) -> [str]:
        """Retrieve all supported measurements names of a specific managed
        object.

        Args:
            mo_id (str):  Managed object ID

        Return:
            List of measurement fragment names.
        """
        result_json = self.c8y.get(self.build_object_path(mo_id) + '/' + ManagedObject.Resource.SUPPORTED_MEASUREMENTS)
        return result_json[ManagedObject.Fragment.SUPPORTED_MEASUREMENTS]

    def get_supported_series(self, mo_id) -> [str]:
        """Retrieve all supported measurement series names of a specific
        managed object.

        Args:
            mo_id (str):  Managed object ID

        Return:
            List of series names.
        """
        result_json = self.c8y.get(self.build_object_path(mo_id) + '/' + ManagedObject.Resource.SUPPORTED_SERIES)
        return result_json[ManagedObject.Fragment.SUPPORTED_SERIES]


class DeviceInventory(Inventory):
    """Provides access to the Device Inventory API.

    This class can be used for get, search for, create, update and
    delete device objects within the Cumulocity database.

    See also: https://cumulocity.com/api/#tag/Inventory-API
    """

    def request(self, id: str):  # noqa (id)
        """ Create a device request.

        Args:
            id (str): Unique ID of the device (e.g. Serial, IMEI); this is
            _not_ the database ID.
        """
        self.c8y.post('/devicecontrol/newDeviceRequests', {'id': id})

    def accept(self, id: str):  # noqa (id)
        """ Accept a device request.

        Args:
            id (str): Unique ID of the device (e.g. Serial, IMEI); this is
            _not_ the database ID.
        """
        self.c8y.put('/devicecontrol/newDeviceRequests/' + str(id), {'status': 'ACCEPTED'})

    def get(self, id: str) -> Device:  # noqa (id)
        """ Retrieve a specific device object.

        Args:
            id (str): ID of the device object

        Returns:
            A Device instance

        Raises:
            KeyError:  if the ID is not defined within the database
        """
        device = Device.from_json(self._get_object(id))
        device.c8y = self.c8y
        return device

    @classmethod
    def _prepare_device_query_param(cls, query: str) -> str:
        if query:
            # insert after opening bracket or at the beginning
            insert_at = query.find('filter=', ) + 1
            query = query[:insert_at] + "has(c8y_IsDevice) and " + query[insert_at:]
        return query

    def select(  # noqa (order)
            self,
            expression: str = None,
            query: str = None,
            ids: list[str | int] = None,
            order_by: str = None,
            type: str = None,
            parent: str = None,
            fragment: str = None,
            fragments: list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            only_roots: str = None,
            with_children: bool = None,
            with_children_count: bool = None,
            skip_children_names: bool = None,
            with_groups: bool = None,
            with_parents: bool = None,
            with_latest_values: bool = None,
            limit: int = None,
            page_size: int = 100,
            page_number: int = None,
            **kwargs,) -> Generator[Device]:
        # pylint: disable=arguments-differ, arguments-renamed
        """ Query the database for devices and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification.  Filters can be
        combined (within reason).

        Note: this variant doesn't allow filtering by fragment because the
        `c8y_IsDevice` fragment is automatically filtered.

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            query (str):  Complex query to execute; all other filters are
                ignored if such a custom query is provided
            ids (List[str|int]): Specific object ID to select.
            type (str):  Device type
            parent (str):  Parent object in the asset hierarchy (ID).
            order_by (str):  Field/expression to sort the results.
            fragment (str):  Name of a present custom/standard fragment
            fragments (list[str]): Additional fragments present within objects
            name (str):  Name of the device
                Note: The Cumulocity REST API does not support filtering for
                names directly; this is a convenience parameter which will
                translate all filters into a query string.
            owner (str):  Username of the object owner
            text (str): Text value of any object property.
            only_roots (bool): Whether to include only objects that don't have
                any parent
            with_children (bool):  Whether children with ID and name should be
                included with each returned object
            with_children_count (bool): When set to true, the returned result
                will contain the total number of children in the respective
                child additions, assets and devices sub fragments.
            skip_children_names (bool):  If true, returned references of child
                devices won't contain their names.
            with_groups (bool): Whether to include additional information about
                the groups to which the searched managed object belongs to.
                This results in setting the assetParents property with
                additional information about the groups.
            with_parents (bool): Whether to include a device's parents.
            with_latest_values (bool):  If true the platform includes the
                fragment `c8y_LatestMeasurements, which contains the latest
                measurement values reported by the device to the platform.
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of events which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator for Device objects
        """
        return super()._select(
            Device.from_json,
            device_mode=True,
            expression=expression,
            query=query,
            ids=ids,
            order_by=order_by,
            type=type,
            parent=parent,
            fragment=fragment,
            fragments=fragments,
            name=name,
            owner=owner,
            text=text,
            only_roots=only_roots,
            with_children=with_children,
            with_children_count=with_children_count,
            skip_children_names=skip_children_names,
            with_groups=with_groups,
            with_parents=with_parents,
            with_latest_values=with_latest_values,
            limit=limit,
            page_size=page_size,
            page_number=page_number,
            **kwargs)

    def get_all(  # noqa (changed signature)
            self,
            expression: str = None,
            query: str = None,
            ids: list[str | int] = None,
            order_by: str = None,
            type: str = None,
            parent: str = None,
            fragment: str = None,
            fragments: list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            only_roots: str = None,
            with_children: bool = None,
            with_children_count: bool = None,
            skip_children_names: bool = None,
            with_groups: bool = None,
            with_parents: bool = None,
            with_latest_values: bool = None,
            limit: int = None,
            page_size: int = 100,
            page_number: int = None,
            **kwargs) -> List[Device]:
        # pylint: disable=arguments-differ, arguments-renamed
        """ Query the database for devices and return the results as list.

        This function is a greedy version of the `select` function. All
        available results are read immediately and returned as list.

        Returns:
            List of Device objects
        """
        return list(self.select(
            expression=expression,
            query=query,
            ids=ids,
            order_by=order_by,
            type=type,
            parent=parent,
            fragment=fragment,
            fragments=fragments,
            name=name,
            owner=owner,
            text=text,
            only_roots=only_roots,
            with_children=with_children,
            with_children_count=with_children_count,
            skip_children_names=skip_children_names,
            with_groups=with_groups,
            with_parents=with_parents,
            with_latest_values=with_latest_values,
            limit=limit,
            page_size=page_size,
            page_number=page_number,
            **kwargs))

    def get_count(  # noqa (changed signature)
            self,
            expression: str = None,
            query: str = None,
            ids: list[str | int] = None,
            type: str = None,
            parent: str = None,
            fragment: str = None,
            fragments: list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            **kwargs) -> int:
        # pylint: disable=arguments-differ, arguments-renamed
        """Calculate the number of potential results of a database query.

        This function uses the same parameters as the `select` function.

        Returns:
            Number of potential results
        """
        return self._get_count(self._prepare_inventory_query(
            device_mode=True,
            expression=expression,
            query=query,
            ids=ids,
            type=type,
            parent=parent,
            fragment=fragment,
            fragments=fragments,
            name=name,
            owner=owner,
            text=text,
            page_size=1,
            **kwargs))

    def delete(self, *devices: Device):
        """ Delete one or more devices and the corresponding within the database.

        The objects can be specified as instances of a database object
        (then, the id field needs to be defined) or simply as ID (integers
        or strings).

        Note: In contrast to the regular `delete` function defined in class
        ManagedObject, this version also removes the corresponding device
        user from database.

        Args:
           *devices (Device): Device objects within the database specified
                (with defined ID).
        """
        for d in devices:
            d.delete()


class DeviceGroupInventory(Inventory):
    """Provides access to the Device Groups Inventory API.

    This class can be used for get, search for, create, update and
    delete device groups within the Cumulocity database.

    See also: https://cumulocity.com/api/#tag/Inventory-API
    """

    def get(self, group_id: str):
        # pylint: disable=arguments-differ, arguments-renamed
        """ Retrieve a specific device group object.

        Args:
            group_id (str):  ID of the device group object.

        Returns:
            DeviceGroup instance.

        Raises:
            KeyError:  if the ID is not defined within the database.
        """
        group = DeviceGroup.from_json(self._get_object(group_id))
        group.c8y = self.c8y
        return group

    def select(  # noqa (changed signature)
            self,
            expression: str = None,
            query: str = None,
            ids: List[str | int] = None,
            order_by: str = None,
            type: str = None,
            parent: str | int = None,
            fragment: str = None,
            fragments: list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            only_roots: str = None,
            with_children: bool = None,
            with_children_count: bool = None,
            skip_children_names: bool = None,
            with_groups: bool = None,
            with_parents: bool = None,
            with_latest_values: bool = None,
            limit: int = None,
            page_size: int = 100,
            page_number: int = None,
            **kwargs) -> Generator[DeviceGroup]:
        # pylint: disable=arguments-differ, arguments-renamed
        """ Select device groups by various parameters.

        This is a lazy implementation; results are fetched in pages but
        parsed and returned one by one.

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            query (str):  Complex query to execute; all other filters are
                ignored if such a custom query is provided
            ids (List[str|int]): Specific object ID to select
            order_by (str):  Field/expression to sort the results.
            fragment (str):  Name of a present custom/standard fragment
            fragments (list[str]): Additional fragments present within objects
            type (bool):  Filter for root or child groups respectively.
                Note: If set to None, no type filter will be applied which
                will match all kinds of managed objects. If you want to
                match device groups only you need to use the fragment filter.
            parent (str):  Parent object in the asset hierarchy (ID).
                Note: this sets the `type` filter to be c8y_DeviceSubGroup
                if not defined; Like the `name` parameter, this is a
                convenience parameter which will translate all filters into
                a query string.
            fragment (str): Additional fragment present within the objects
            fragments (list[str]): Additional fragments present within the objects
            name (str): Name string of the groups to select
                Note:  he Cumulocity REST API does not support filtering for
                names directly; this is a convenience parameter which will
                translate all filters into a query string.
                No partial matching/patterns are supported
            owner (str): Username of the group owner
            text (str): Text value of any object property.
            only_roots (bool): Whether to include only objects that don't have
                any parent
            with_children (bool):  Whether children with ID and name should be
                included with each returned object
            with_children_count (bool): When set to true, the returned result
                will contain the total number of children in the respective
                child additions, assets and devices sub fragments.
            skip_children_names (bool):  If true, returned references of child
                devices won't contain their names.
            with_groups (bool): Whether to include additional information about
                the groups to which the searched managed object belongs to.
                This results in setting the assetParents property with
                additional information about the groups.
            with_parents (bool): Whether to include a device's parents.
            with_latest_values (bool):  If true the platform includes the
                fragment `c8y_LatestMeasurements, which contains the latest
                measurement values reported by the device to the platform.
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of events which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator of DeviceGroup instances
        """
        type = type or (DeviceGroup.CHILD_TYPE if parent else None)

        return super()._select(
            parse_fun=DeviceGroup.from_json,
            device_mode=False,
            expression=expression,
            query=query,
            ids=ids,
            order_by=order_by,
            type=type,
            parent=parent,
            fragment=fragment,
            fragments=fragments,
            name=name,
            owner=owner,
            text=text,
            only_roots=only_roots,
            with_children=with_children,
            with_children_count=with_children_count,
            skip_children_names=skip_children_names,
            with_groups=with_groups,
            with_parents=with_parents,
            with_latest_values=with_latest_values,
            limit=limit,
            page_size=page_size,
            page_number=page_number,
            **kwargs)

    def get_count(  # noqa (changed signature)
            self,
            expression: str = None,
            query: str = None,
            ids: list[str | int] = None,
            parent: str | int = None,
            type: str = None,
            fragment: str = None,
            fragments: list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            **kwargs) -> int:
        # pylint: disable=arguments-differ, arguments-renamed
        """Calculate the number of potential results of a database query.

        This function uses the same parameters as the `select` function.

        Returns:
            Number of potential results
        """
        type = type or (DeviceGroup.CHILD_TYPE if parent else None)
        base_query = self._prepare_inventory_query(
            device_mode=False,
            expression=expression,
            query=query,
            ids=ids,
            type=type,
            parent=parent,
            fragment=fragment,
            fragments=fragments,
            name=name,
            owner=owner,
            text=text,
            page_size=1,
            **kwargs)
        return self._get_count(base_query)

    def get_all(  # noqa (changed signature)
            self,
            expression: str = None,
            query: str = None,
            ids: list[str | int] = None,
            type: str = None,
            parent: str | int = None,
            fragment: str = None,
            fragments: list[str] = None,
            name: str = None,
            owner: str = None,
            text: str = None,
            only_roots: str = None,
            with_children: bool = None,
            with_children_count: bool = None,
            skip_children_names: bool = None,
            with_groups: bool = None,
            with_parents: bool = None,
            with_latest_values: bool = None,
            limit: int = None,
            page_size: int = 100,
            page_number: int = None,
            **kwargs) -> List[DeviceGroup]:
        # pylint: disable=arguments-differ, arguments-renamed
        """ Select managed objects by various parameters.

        In contract to the select method this version is not lazy. It will
        collect the entire result set before returning.

        Returns:
            List of DeviceGroup instances.
        """
        return list(self.select(
            expression=expression,
            type=type,
            query=query,
            ids=ids,
            parent=parent,
            fragment=fragment,
            fragments=fragments,
            name=name,
            owner=owner,
            text=text,
            only_roots=only_roots,
            with_children=with_children,
            with_children_count=with_children_count,
            skip_children_names=skip_children_names,
            with_groups=with_groups,
            with_parents=with_parents,
            with_latest_values=with_latest_values,
            limit=limit,
            page_size=page_size,
            page_number=page_number,
            **kwargs))

    def create(self, *groups):
        """Batch create a collection of groups and entire group trees.

        Args:
            *groups (DeviceGroup):  collection of DeviceGroup instances;
                each can define children as needed.
        """
        super()._create(DeviceGroup.to_json, *groups)

    def assign_children(self, root_id: str, *child_ids: str):
        """Link child groups to this device group.

        Args:
            root_id (str): ID of the root device group.
            *child_ids (str): Collection of the child device group ID.
        """
        # adding multiple references at once is not (yet) supported
        # refs = {'references': [InventoryUtil.build_managed_object_reference(id) for id in child_ids]}
        # self.c8y.post(self.build_object_path(root_id) + '/childAssets', json=refs, accept='')
        for child_id in child_ids:
            self.c8y.post(self.build_object_path(root_id) + '/childAssets',
                          json=ManagedObjectUtil.build_managed_object_reference(child_id), accept='')

    def unassign_children(self, root_id, *child_ids):
        """Unlink child groups from this device group.

        Args:
            root_id (str): ID of the root device group.
            *child_ids (str): Collection of the child device group ID.
        """
        refs = {'references': [ManagedObjectUtil.build_managed_object_reference(i) for i in child_ids]}
        self.c8y.delete(self.build_object_path(root_id) + '/childAssets', json=refs)

    def delete(self, *groups: DeviceGroup | str):
        """Delete one or more single device groups within the database.

        The child groups (if there are any) are left dangling. This is
        equivalent to using the `cascade=false` parameter in the
        Cumulocity REST API.

        Args:
            *groups (str|DeviceGroup):  Collection of objects (or ID).
        """
        self._delete(False, *groups)

    def delete_trees(self, *groups: DeviceGroup | str):
        """Delete one or more device groups trees within the database.

        This is equivalent to using the `cascade=true` parameter in the
        Cumulocity REST API.

        Args:
            *groups (str|DeviceGroup):  Collection of objects (or ID).
        """
        self._delete(False, *groups)

    def _delete(self, cascade: bool, *objects: DeviceGroup | str):
        try:
            object_ids = [o.id for o in objects]  # noqa (id)
        except AttributeError:
            object_ids = objects
        for object_id in object_ids:
            self.c8y.delete(self.build_object_path(object_id) + f"?cascade={'true' if cascade else 'false'}")
