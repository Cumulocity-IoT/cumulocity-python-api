from datetime import datetime, timezone, timedelta
from typing import Any


def assert_c8y(obj):
    """Assert that a model object has a Cumulocity connection."""
    if not obj.c8y:
        raise ValueError("Cumulocity connection reference must be set to allow direct database access.")


def assert_id(obj):
    """Assert that a model object has a Cumulocity connection."""
    if not obj.id:
        raise ValueError("The object ID must be set to allow direct object access.")


def get_by_path(dictionary: dict, path: str, default: Any = None, fail: bool = False) -> Any:
    """Select a nested value from a dictionary by path-like expression
    (dot notation).

    Args:
        dictionary (dict):  the dictionary to extract values from
        path (str):  a path-like expressions
        default (Any):  default value to return if the path expression
            doesn't match a value in the dictionary.
        fail (bool):  whether to raise an exception if the path expression
            doesn't match a value in the dictionary.

    Return:
        The extracted value or the specified default.
    """
    keys = path.split('.')
    current = dictionary

    for key in keys:
        if not isinstance(current, dict):
            return default
        if key in current:
            current = current[key]
            continue
        pascal_key = to_pascal_case(key)
        if pascal_key in current:
            current = current[pascal_key]
            continue
        if fail:
            raise KeyError(f"Unable to find '{path}' in object JSON.")
        return default

    return current


def as_tuple(data: dict, paths: list[str | tuple]) -> tuple:
    """Select nested values from a dictionary by path-like expressions
    (dot notation) and return as tuple.

    Args:
        data (dict):  the dictionary to extract values from
        paths: (list):  a list of path-like expressions; each "expression"
            can be a tuple to define a default value other than None.

    Return:
        The extracted values (or defaults it specified) as tuple. The
        number of elements in the tuple matches the length of the `paths`
        argument.
    """
    if isinstance(paths, list):
        return tuple(
            get_by_path(
                data,
                path[0] if isinstance(path, tuple) else path,
                path[1] if isinstance(path, tuple) else None
            )
            for path in paths
        )
    return get_by_path(
        data,
        paths[0] if isinstance(paths, tuple) else paths,
        paths[1] if isinstance(paths, tuple) else None
    )


def as_record(data: dict, mapping: dict[str, str | tuple[str | Any]]) -> dict:
    """Select nested values from a dictionary by path-like expressions
    (dot notation) and return as record (dict).

    Args:
        data (dict):  the dictionary to extract values from
        mapping: (dict):  a dictionary mapping result keys to a path-like
            expression; each "expression" can be a tuple to define a
            default value other than None.

    Return:
        The extracted values (or defaults it specified) as dictionary.
    """
    return {
        key: get_by_path(
            data,
            path[0] if isinstance(path, tuple) else path,
            path[1] if isinstance(path, tuple) else None
        )
        for key, path in mapping.items()
    }


def to_datetime(value: str | None) -> datetime:
    """Convert a Cumulocity datetime object to a datetime."""


def to_timestring(value: datetime | None) -> str:
    """Convert a Cumulocity timestring object to a string."""


def to_pascal_case(name: str) -> str:
    """Convert a given snake case (default Python style) name to pascal case (default for names in Cumulocity)"""
    parts = list(filter(None, name.split('_')))
    if len(parts) == 1:
        return name
    return parts[0] + "".join([x.title() for x in parts[1:]])