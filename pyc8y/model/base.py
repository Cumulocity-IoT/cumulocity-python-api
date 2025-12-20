from abc import ABC
from typing import Any, Generic, TypeVar, Self, Coroutine

from pyc8y.base import CumulocityRestApi
from pyc8y.model.util import assert_c8y, assert_id, as_tuple, as_record, get_by_path, to_datetime

T = TypeVar("T")


def json_property(key: str, read_only=False) -> property:
    def getter(self):
        return self._json[key]
    def setter(self, value):
        self._update_json[key] = value
    return property(getter) if read_only else property(getter, setter)


def id_property(key: str, read_only=False) -> property:
    def getter(self):
        return self._json[key]["id"]
    def setter(self, value):
        self._update_json["source"] = {"id": value}
    return property(getter) if read_only else property(getter, setter)


def datetime_property(key: str) -> property:
    def getter(self):
        return to_datetime(self._json[key])
    return property(getter)


class CumulocityObject(ABC, Generic[T]):
    """Base class for all Cumulocity database objects."""

    _resource_name: str = None
    _mime_type: str = None

    def __init__(self, c8y: CumulocityRestApi | None, **kwargs):
        self.c8y = c8y
        self._source_json: dict | None = None
        self._update_json: dict = {}

    @property
    def _json(self) -> dict:
        if not self._update_json:
            return self._source_json
        return self._source_json | self._update_json

    @property
    def id(self):
        return self._json["id"]

    @property
    def resource_path(self) -> str:
        return f"{self._resource_name}/{self.id}"

    def __repr__(self) -> str:
        return ''.join([   # -> ClassName(id=123, type=abc)
            type(self).__name__,
            "(",
            ", ".join([
                f"{n}={getattr(self, n)}"
                for n in ["id", "type"] if getattr(self, n) is not None
            ]),
            ")"
        ])

    @classmethod
    def from_json(cls, json: dict) -> Self:
        obj = type(cls)()
        obj._source_json = json
        return obj

    def to_json(self, only_updated=False) -> dict:
        return self._update_json if only_updated else self._json

    def __getitem__(self, item) -> Any:
        return get_by_path(self._json, item, fail=True)

    def get(self, item, default: any = None) -> any:
        return get_by_path(self._json, item, default=default)

    def as_tuple(self, *paths: str | tuple[str, Any]) -> tuple:
        return as_tuple(self._json, *paths)

    def as_record(self, mapping: dict[str, str | tuple[str | Any]]) -> dict:
        return as_record(self._json, mapping)

    async def create(self) -> Self:
        """Create the object within the database.

        Returns:
            A fresh object representing what was created within the database;
            this includes the Cumulocity ID.
        """
        assert_c8y(self)
        result = self.from_json(await self.c8y.post(
            self.resource_path,
            json=self.to_json(),
            accept=self._mime_type
        ))
        result.c8y = self.c8y
        return result

    async def update(self) -> Self:
        """Update the object within the database.

        Note: This will only send changed fields to increase performance.

        Returns:
            A fresh object representing the updated state within the database.
        """
        assert_c8y(self)
        assert_id(self)
        result = self.from_json(await self.c8y.put(
            self.resource_path,
            json=self.to_json(True),
            accept=self._mime_type,
            content_type=self._mime_type))
        result.c8y = self.c8y
        return result

    async def apply_to(self, other_id: str | int) -> Self:
        """Apply changes made to this object to another object in the database.

        Args:
            other_id (str):  Database ID of the event to update.

        Returns:
            A fresh object representing the updated object's state within
            the database.
        """
        assert_c8y(self)
        result = self.from_json(await self.c8y.put(
            f"{self._resource_name}/{other_id}",
            json=self.to_json(True),
            accept=self._mime_type,
            content_type=self._mime_type))
        result.c8y = self.c8y
        return result

    def _delete(self, **params) -> Coroutine:
        assert_c8y(self)
        assert_id(self)
        return self.c8y.delete(self.resource_path, params=params)

    async def delete(self, **_) -> None:  # allow override with parameters
        """Delete the object within the database."""
        await self._delete()
