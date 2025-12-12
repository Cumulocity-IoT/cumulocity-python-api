# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

from typing import Set

from c8y_api.model._base import ComplexObject


class SimpleObjectParser(object):
    """A parser for simple (without fragments) Cumulocity database objects.

    The parser converts between an object and a JSON representation using
    a simple field mapping dictionary.
    """

    def __init__(self, mapping: dict = None, **kwargs):
        if mapping is None:
            mapping = {}
        self._obj_to_json = {**mapping, 'id': 'id', **kwargs}
        self._json_to_object = {v: k for k, v in self._obj_to_json.items()}

    def from_json(self, obj_json, new_obj, skip=None):
        """Update a given object instance with data from a JSON object.

        This function uses the parser's mapping definition, only fields
        are parsed that are part if this.

        Use the skip list to skip certain objects fields within the update
        regardless whether they are defined in the mapping.

        Args:
            obj_json: JSON object (nested dict) to parse
            new_obj:  object instance to update (usually newly created)
            skip:  list of object field names to skip or None if nothing
                should be skipped

        Returns:
            The updated object instance.
        """
        for json_key, field_name in self._json_to_object.items():
            if not skip or field_name not in skip:
                if json_key in obj_json:
                    new_obj.__dict__[field_name] = obj_json[json_key]
        return new_obj

    def to_json(self, obj: object, include=None, exclude=None):
        """Build a JSON representation of an object.

        Use the include list to limit the represented fields to a specific
        subset (e.g. just the updated fields). Use the exclude list to ignore
        certain fields in the representation.

        If a field is present in both lists, it will be excluded.

        Args:
            obj (object): the object to represent in JSON format.
            include:  an iterable of object fields to include or None if all
                fields should be included.
            exclude:  an iterable of object fields to exclude or None of no
                field should be excluded.

        Returns:
            A JSON representation (nested dict) of the object.
        """
        obj_json = {}
        for name, value in obj.__dict__.items():
            if include is None or name in include:  # field is included
                if exclude is None or name not in exclude:  # field is not included
                    if value is not None and name in self._obj_to_json:
                        obj_json[self._obj_to_json[name]] = value
        return obj_json


class ComplexObjectParser(SimpleObjectParser):
    """A parser for complex (with fragments) Cumulocity database objects.

    The parser converts between an object and a JSON representation using
    a simple field mapping dictionary. All other fields are mapped as
    fragments, an exclusive list can be given to skip unwanted fields.
    """

    def __init__(self, to_json_mapping, no_fragments_list):
        super().__init__(to_json_mapping)
        self._ignore_as_fragments = {*no_fragments_list, *to_json_mapping.values(), 'self', 'id'}

    def from_json(self, obj_json, new_obj, skip=None):
        new_obj = super().from_json(obj_json, new_obj)
        new_obj.fragments = self._parse_fragments(obj_json, self._ignore_as_fragments)
        return new_obj

    def to_json(self, obj: ComplexObject, include=None, exclude=None):
        obj_json = super().to_json(obj, include, exclude)
        if include is None:
            obj_json.update(self._format_fragments(obj))
        else:
            included = obj.get_updates()
            obj_json.update(self._format_fragments(obj, include=included))
        return obj_json

    @staticmethod
    def _parse_fragments(obj_json, ignore: Set[str]):
        return {name: body for name, body in obj_json.items() if name not in ignore}

    @staticmethod
    def _format_fragments(obj: ComplexObject, include: Set[str] | None = None) -> dict:
        if include is None:
            return dict(obj.fragments.items())
        return {name: fragment for name, fragment in obj.fragments.items() if name in include}
