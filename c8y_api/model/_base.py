# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

import logging
from typing import Any, Iterable, Set
from urllib.parse import quote_plus, urlencode

from collections.abc import MutableMapping
from deprecated import deprecated

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model._util import _DateUtil, _StringUtil, _QueryUtil


class _DictWrapper(MutableMapping):

    def __init__(self, dictionary: dict, on_update=None):
        self.__dict__['_property_items'] = dictionary
        self.__dict__['_property_on_update'] = on_update

    def has(self, name: str):
        """Check whether a key is present in the dictionary."""
        return name in self.__dict__['_property_items']

    def __getitem__(self, name):
        item = self.__dict__['_property_items'][name]
        return item if not isinstance(item, dict) else _DictWrapper(item, self.__dict__['_property_on_update'])

    def __setitem__(self, name, value):
        self.__dict__['_property_items'][name] = value

    def __delitem__(self, _):
        raise NotImplementedError

    def __iter__(self):
        return iter(self.__dict__['_property_items'])

    def __len__(self):
        return len(self.__dict__['_property_items'])

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(
                f"'{type(self).__name__}' object has no attribute '{name}'"
            ) from None

    def __setattr__(self, name, value):
        if self.__dict__['_property_on_update']:
            self.__dict__['_property_on_update']()
        self[name] = value

    def __str__(self):
        return self.__dict__['_property_items'].__str__()


class CumulocityObject:
    """Base class for all Cumulocity database objects."""

    def __init__(self, c8y: CumulocityRestApi | None):
        self.c8y = c8y
        self.id: str | None = None

    def _assert_c8y(self):
        if not self.c8y:
            raise ValueError("Cumulocity connection reference must be set to allow direct database access.")

    def _assert_id(self):
        if not self.id:
            raise ValueError("The object ID must be set to allow direct object access.")

    @classmethod
    def _to_datetime(cls, timestring):
        if timestring:
            return _DateUtil.to_datetime(timestring)
        return None


class CumulocityObjectParser:
    """Common base for all Cumulocity object parsers."""

    def from_json(self, obj_json: dict, new_obj: Any, skip: Iterable[str] = None) -> Any[CumulocityObject]:
        """Update a given object instance with data from a JSON object.

        This function uses the parser's mapping definition, only fields
        are parsed that are part if this.

        Use the skip list to skip certain objects fields within the update
        regardless whether they are defined in the mapping.

        Args:
            obj_json (dict): JSON object (nested dict) to parse.
            new_obj (Any):  Object instance to update (usually newly created).
            skip (Iterable):  Collection of object field names to skip
                or None if nothing should be skipped.

        Returns:
            The updated object instance.
        """

    def to_json(self, obj: Any, include: Iterable[str] = None, exclude: Iterable[str] = None) -> dict:
        """Build a JSON representation of an object.

        Use the include list to limit the represented fields to a specific
        subset (e.g. just the updated fields). Use the exclude list to ignore
        certain fields in the representation.

        If a field is present in both lists, it will be excluded.

        Args:
            obj (Any):  the object to format as JSON.
            include (Iterable):  Collection of object fields to include
                or None if all fields should be included.
            exclude (Iterable):  Collection of object fields to exclude
                or None of no field should be included.

        Returns:
            A JSON representation (nested dict) of the object.
        """


class SimpleObject(CumulocityObject):
    """Base class for all simple Cumulocity objects (without custom fragments)."""

    # Note: SimpleObject derives from multiple base classes. The last does
    # not need to be aware of this, all others are passing unknown initialization
    # arguments (kwargs) to other super classes. Hence, the order of super
    # classes is relevant

    _parser = CumulocityObjectParser()
    _not_updatable = set()
    _resource = ''
    _accept = None

    class UpdatableProperty:
        """Updatable property."""
        # Providing updatable properties for SimpleObject instances.
        # An updatable property is watched - write access will be recorded
        # within the SimpleObject instance to be able to provide incremental
        # updates to objects within Cumulocity."""

        def __init__(self, name):
            self.internal_name = name

        def __get__(self, obj, _):
            return obj.__dict__[self.internal_name]

        def __set__(self, obj, value):
            # pylint: disable=protected-access
            obj._signal_updated_field(self.internal_name)
            obj.__dict__[self.internal_name] = value

        def __delete__(self, obj):
            # pylint: disable=protected-access
            obj._signal_updated_field(self.internal_name)
            obj.__dict__[self.internal_name] = None

    def __init__(self, c8y: CumulocityRestApi | None):
        super().__init__(c8y=c8y)
        self._updated_fields = None

    def _build_resource_path(self):
        """Get the resource path.

        This method is used by the internal `_create`, `_update`, `_delete`
        methods and alike. The resource path does not include leading or
        trailing '/' characters.

        By default, this is just static the class `_resource` field, but it
        can be customized in derived classes if this needs to be dynamic.
        """
        return self._resource

    def _build_object_path(self):
        """Get the object path.

        This method is used by the internal `_create`, `_update`, `_delete`
        methods and alike. The object path does not include leading or
        trailing '/' characters.

        By default, this is just the class `_resource` field plus object ID,
        but it can be customized if this needs to be dynamic.
        """
        # no need to assert the ID - this function is only used when
        # the database ID is defined
        return self._build_resource_path() + '/' + str(self.id)

    @classmethod
    def from_json(cls, json: dict) -> Any[SimpleObject]:
        """Create an object instance from Cumulocity JSON format.

        Caveat: this function is primarily for internal use and does not
        return a full representation of the JSON. It is used for object
        creation and update within Cumulocity.

        Args:
            json (dict): The JSON to parse.

        Returns:
            A CumulocityObject instance.
        """
        # The from_json function must be implemented in the subclass

    def to_json(self, only_updated=False) -> dict:
        """Create a representation of this object in Cumulocity JSON format.

        Caveat: this function is primarily for internal use and does not
        return a full representation of the object. It is used for object
        creation and update within Cumulocity, so for example the 'id'
        field is never included.

        Args:
            only_updated (bool):  Whether the result should be limited to
                changed fields only (for object updates). Default: `False`

        Returns:
            A JSON (nested dict) object.
        """
        return self._to_json(only_updated, self._not_updatable)

    def to_full_json(self) -> dict:
        """Create a complete representation of this object in
        Cumulocity JSON format.

        This representation is used for object creation and when a model
        object is applied to another.

        Note: this is just a shortcut for `to_json()`

        Returns:
            A JSON (nested dict) object.
        """
        return self.to_json()

    def to_diff_json(self) -> dict:
        """Create a complete representation of this object in
        Cumulocity JSON format.

        This representation is used for object updates (not when a model
        object is applied to another).

        Note: this is just a shortcut for `to_json(True)`

        Returns:
            A JSON (nested dict) object.
        """
        return self.to_json(only_updated=True)

    def get_updates(self) -> set[str]:
        """Get the names of updated fields.

        Returns:
            A set of (internal) field names that where updated after
            object creation.
        """
        return self._updated_fields or set()

    @classmethod
    def _from_json(cls, json: dict, obj: SimpleObject) -> Any[SimpleObject]:
        return cls._parser.from_json(json, obj)

    def _to_json(self, only_updated=False, exclude: Set[str] = None) -> dict:
        include = None if not only_updated else self._updated_fields if self._updated_fields else set()
        exclude = {'id', *(exclude or {})}
        return self._parser.to_json(self, include, exclude)

    def _signal_updated_field(self, internal_name):
        if not self._updated_fields:
            self._updated_fields = {internal_name}
        else:
            self._updated_fields.add(internal_name)

    def _create(self) -> Any[SimpleObject]:
        self._assert_c8y()
        result_json = self.c8y.post(self._build_resource_path(),
                                    self.to_json(), accept=self._accept)
        result = self.from_json(result_json)
        result.c8y = self.c8y
        return result

    def _update(self) -> Any[SimpleObject]:
        self._assert_c8y()
        self._assert_id()
        result_json = self.c8y.put(self._build_object_path(), self.to_json(True), accept=self._accept)
        result = self.from_json(result_json)
        result.c8y = self.c8y
        return result

    def _delete(self):
        self._assert_c8y()
        self._assert_id()
        self.c8y.delete(self._build_object_path())

    def delete(self):
        """Delete the object within the database."""
        self._delete()


class ComplexObject(SimpleObject):
    """Abstract base class for all complex cumulocity objects
    (that can have custom fragments)."""
    # pylint: disable=unnecessary-dunder-call

    log = logging.getLogger(__name__)

    def __init__(self, c8y: CumulocityRestApi | None, **kwargs):
        super().__init__(c8y)
        self._updated_fragments = None
        self.fragments = {}
        for key, value in kwargs.items():
            self.fragments[key] = value
        self.__setattr__ = self._setattr_

    def __setitem__(self, name: str, fragment: str | bool | int | float | dict | list):
        """ Add/set a custom fragment.

        The fragment value can be a simple value or any JSON-like structure
        (specified as nested dictionary).::

            obj['c8y_SimpleValue'] = 14
            obj['c8y_ComplexValue'] = { 'x': 1, 'y': 2, 'text': 'message'}

        Args:
            name (str):  Name of the custom fragment.
            fragment (str|bool|int|float|dict):  custom value/structure to assign.
        """
        pascal_name = _StringUtil.to_pascal_case(name)
        if pascal_name in self.fragments:
            self.fragments[pascal_name] = fragment
            self._signal_updated_fragment(pascal_name)
        else:
            self.fragments[name] = fragment
            self._signal_updated_fragment(name)

    def __getitem__(self, name: str):
        """ Get the value of a custom fragment.

        Depending on the definition the value can be a scalar or a
        complex structure (modelled as nested dictionary).

        Access to fragments can also be done in dot notation::
            msg = obj['c8y_Custom']['text']
            msg = obj.c8y_Custom.text

        Args:
            name (str): Name of the custom fragment.
        """
        # A fragment is a simple dictionary. By wrapping it into the _DictWrapper class
        # it is ensured that the same access behaviour is ensured on all levels.
        # All updated anywhere within the dictionary tree will be reported as an update
        # to this instance.
        # If the element is not a dictionary, it can be returned directly
        item = self.fragments[name]
        return item if not isinstance(item, dict) else \
            _DictWrapper(self.fragments[name], lambda: self._signal_updated_fragment(name))

    def __getattr__(self, name: str):
        """ Get the value of a custom fragment.

        Depending on the definition the value can be a scalar or a
        complex structure (modelled as nested dictionary).

        Args:
            name (str): Name of the custom fragment.
        """
        if name in self:
            return self[name]
        pascal_name = _StringUtil.to_pascal_case(name)
        if pascal_name in self:
            return self[pascal_name]
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}' or '{pascal_name}'"
        ) from None

    def _setattr_(self, name, value):
        if name in self.fragments:
            self[name] = value
            return
        pascal_name = _StringUtil.to_pascal_case(name)
        if pascal_name in self.fragments:
            self[pascal_name] = value
            return
        object.__setattr__(self, name, value)

    def __iadd__(self, other):
        try:  # go for iterable
            for i in other:
                self.fragments[i.name] = i.items
                self._signal_updated_fragment(i.name)
        except TypeError:
            self.__iadd__([other])
        return self

    def __contains__(self, name):
        return name in self.fragments

    def get(self, path: str, default=None):
        """Get a fragment/value by path.

        Args:
            path (str): A fragment/value path in dot notation, e.g.
                "c8y_Firmware.version"
            default: Sensible default if the path is not defined.

        Returns:
            The fragment/value specified via the path or the default value
            if the path is not defined.
        """
        segments = path.split('.')
        value = self
        for segment in segments:
            if segment in value:
                value = value[segment]
                continue
            if hasattr(value, segment):
                return value.__getattribute__(segment)
            return default
        return value

    @deprecated
    def set_attribute(self, name, value):
        # pylint: disable=missing-function-docstring
        logging.warning("Function 'set_attribute' is deprecated and will be removed "
                        "in a future release. Please use the [] operator instead.")
        self.__setitem__(name, value)
        return self

    @deprecated
    def add_fragment(self, name, **kwargs):
        # pylint: disable=missing-function-docstring
        logging.warning("Function 'add_fragment' is deprecated and will be removed "
                        "in a future release. Please use the [] or += operator instead.")
        self.__setitem__(name, kwargs)
        return self

    @deprecated
    def add_fragments(self, *fragments):
        # pylint: disable=missing-function-docstring
        logging.warning("Function 'add_fragments' is deprecated and will be removed "
                        "in a future release. Please use the [] or += operator instead.")
        self.__iadd__(fragments)
        return self

    @deprecated
    def has(self, name):
        # pylint: disable=missing-function-docstring
        logging.warning("Function 'has' is deprecated and will be removed "
                        "in a future release. Please use the 'in' operator instead.")
        return self.__contains__(name)

    def get_updates(self):
        # redefinition of the super version
        return ([] if not self._updated_fields else list(self._updated_fields)) \
               + ([] if not self._updated_fragments else list(self._updated_fragments))

    def _signal_updated_fragment(self, name: str):
        if not self._updated_fragments:
            self._updated_fragments = {name}
        else:
            self._updated_fragments.add(name)

    def _apply_to(self, other_id: str) -> Any[ComplexObject]:
        self._assert_c8y()
        # put full json to another object (by ID)
        result_json = self.c8y.put(self._build_resource_path() + '/' + other_id, self.to_full_json())
        result = self.from_json(result_json)
        result.c8y = self.c8y
        return result


class CumulocityResource:
    """Abstract base class for all Cumulocity API resources."""

    def __init__(self, c8y: CumulocityRestApi, resource: str):
        self.c8y = c8y
        # ensure that the resource string starts with a slash and ends without.
        self.resource = '/' + resource.strip('/')
        # the default object name would be the resource path element just before
        # the last event for e.g. /event/events
        self.object_name = self.resource.split('/')[-1]

    def build_object_path(self, object_id: int | str) -> str:
        """Build the path to a specific object of this resource.

        Args:
            object_id (int|str):  Technical ID of the object

        Returns:
            The relative path to the object within Cumulocity.
        """
        return self.resource + '/' + str(object_id)

    @staticmethod
    def _map_params(
            q=None,
            query=None,
            type=None,
            name=None,
            fragment=None,
            source=None,  # noqa (type)
            value=None,
            series=None,
            owner=None,
            device_id=None,
            agent_id=None,
            bulk_id=None,
            ids=None,
            text=None,
            before=None,
            after=None,
            date_from=None,
            date_to=None,
            created_before=None,
            created_after=None,
            created_from=None,
            created_to=None,
            updated_before=None,
            updated_after=None,
            last_updated_from=None,
            last_updated_to=None,
            min_age=None,
            max_age=None,
            with_source_assets=None,
            with_source_devices=None,
            reverse=None,
            page_size=None,
            page_number=None,  # (must not be part of the prepared query)
            **kwargs) -> dict:
        assert not page_number

        def multi(*xs):
            return sum(bool(x) for x in xs) > 1

        if multi(min_age, before, date_to):
            raise ValueError("Only one of 'min_age', 'before' and 'date_to' query parameters must be used.")
        if multi(max_age, after, date_from):
            raise ValueError("Only one of 'max_age', 'after' and 'date_from' query parameters must be used.")
        if multi(created_from, created_after):
            raise ValueError("Only one of 'created_from' and 'created_after' query parameters must be used.")
        if multi(created_to, created_before):
            raise ValueError("Only one of 'created_to' and 'created_before' query parameters must be used.")
        if multi(last_updated_from, updated_after):
            raise ValueError("Only one of 'last_updated_from' and 'updated_after' query parameters must be used.")
        if multi(last_updated_to, updated_before):
            raise ValueError("Only one of 'last_updated_to' and 'updated_before' query parameters must be used.")

        if (not source) and any([with_source_devices, with_source_assets]):
            raise ValueError("Can only include source assets/devices if 'source' parameter is provided.")

            # min_age/max_age should be timedelta objects that can be used for
        # alternative calculation of the before/after parameters
        if min_age:
            min_age = _DateUtil.ensure_timedelta(min_age)
            before = _DateUtil.now() - min_age
        if max_age:
            max_age = _DateUtil.ensure_timedelta(max_age)
            after = _DateUtil.now() - max_age
        # before/after can also be datetime objects,
        # if so they need to be timezone aware
        date_from = _DateUtil.ensure_timestring(date_from) or _DateUtil.ensure_timestring(after)
        date_to = _DateUtil.ensure_timestring(date_to) or _DateUtil.ensure_timestring(before)
        created_from = _DateUtil.ensure_timestring(created_from) or _DateUtil.ensure_timestring(created_after)
        created_to = _DateUtil.ensure_timestring(created_to) or _DateUtil.ensure_timestring(created_before)
        updated_from = _DateUtil.ensure_timestring(last_updated_from) or _DateUtil.ensure_timestring(updated_after)
        updated_to = _DateUtil.ensure_timestring(last_updated_to) or _DateUtil.ensure_timestring(updated_before)

        params = {k: v for k, v in {
            'q': q,
            'query': query,
            'type': type,
            'name': f"'{_QueryUtil.encode_odata_query_value(name)}'" if name else None,
            'owner': owner,
            'source': source,
            'fragmentType': fragment,
            'deviceId': device_id,
            'agentId': agent_id,
            'bulkId': bulk_id,
            'text': f"'{_QueryUtil.encode_odata_query_value(text)}'" if text else None,
            'ids': ','.join(str(i) for i in ids) if ids else None,
            'bulkOperationId': bulk_id,
            'dateFrom': date_from,
            'dateTo': date_to,
            'createdFrom': created_from,
            'createdTo': created_to,
            'lastUpdatedFrom': updated_from,
            'lastUpdatedTo': updated_to,
            'withSourceAssets': with_source_assets,
            'withSourceDevices': with_source_devices,
            'revert': str(reverse) if reverse is not None else None,
            'pageSize': page_size}.items() if v is not None}
        params.update({_StringUtil.to_pascal_case(k): v for k, v in kwargs.items() if v is not None})
        return params

    def _prepare_query(self, resource: str = None, expression: str = None, **kwargs):
        encoded = quote_plus(expression) if expression else urlencode(self._map_params(**kwargs))
        if not encoded:
            return resource or self.resource
        return (resource or self.resource) + '?' + encoded

    def _get_object(self, object_id):
        return self.c8y.get(self.build_object_path(object_id))

    def _get_page(self, base_query: str, page_number: int):
        sep = '&' if '?' in base_query else '?'
        result_json = self.c8y.get(f'{base_query}{sep}currentPage={page_number}')
        return result_json[self.object_name]

    def _get_count(self, base_query: str) -> int:
        # the page_size=1 parameter must already be part of the query string
        result_json = self.c8y.get(base_query + '&withTotalPages=true')
        return result_json['statistics']['totalPages']

    def _iterate(self, base_query: str, page_number: int | None, limit: int | None, parse_fun):
        # if no specific page is defined we just start at 1
        current_page = page_number if page_number else 1
        # we will read page after page until
        #  - we reached the limit, or
        #  - there is no result (i.e. we were at the last page)
        num_results = 0
        while True:
            results = [parse_fun(x) for x in self._get_page(base_query, current_page)]
            # no results, so we are done
            if not results:
                break
            for result in results:
                if limit and num_results >= limit:
                    return
                result.c8y = self.c8y  # inject c8y connection into instance
                yield result
                num_results = num_results + 1
            # when a specific page was specified we don't read more pages
            if page_number:
                break
            # continue with next page
            current_page = current_page + 1

    def _create(self, jsonify_func, *objects):
        for o in objects:
            self.c8y.post(self.resource, json=jsonify_func(o), accept=None)

    def _create_bulk(self, jsonify_func, collection_name, content_type, *objects):
        bulk_json = {collection_name: [jsonify_func(o) for o in objects]}
        self.c8y.post(self.resource, bulk_json, content_type=content_type)

    def _update(self, jsonify_func, *objects):
        for o in objects:
            self.c8y.put(self.resource + '/' + str(o.id), json=jsonify_func(o), accept=None)

    def _apply_to(self, jsonify_func, model: dict|Any, *object_ids):
        model_json = model if isinstance(model, dict) else jsonify_func(model)
        for object_id in object_ids:
            self.c8y.put(self.resource + '/' + str(object_id), model_json, accept=None)

    # this one should be ok for all implementations, hence we define it here
    def delete(self, *objects: str):
        """ Delete one or more objects within the database.

        The objects can be specified as instances of a database object
        (then, the id field needs to be defined) or simply as ID (integers
        or strings).

        Args:
            *objects (str):  Objects within the database specified by ID
        """
        try:
            object_ids = [o.id for o in objects]  # noqa (id)
        except AttributeError:
            object_ids = objects
        for object_id in object_ids:
            self.c8y.delete(self.build_object_path(object_id))
