# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

from typing import BinaryIO, Generator, List

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model._base import CumulocityResource, as_tuple
from c8y_api.model.inventory import ManagedObject
from c8y_api.model.matcher import JsonMatcher


class Binary(ManagedObject):
    """Represents a binary object/file within the Database.

    See also: https://cumulocity.com/api/#tag/Binaries
    """

    _resource = '/inventory/binaries'

    def __init__(self, c8y: CumulocityRestApi = None,
                 type: str = None, name: str = None, owner: str = None,  # noqa
                 content_type: str = None, file: str | BinaryIO = None, **kwargs):
        super().__init__(c8y, type=type, name=name, owner=owner, contentType=content_type or type, **kwargs)
        self.file = file

    @property
    def content_type(self) -> str:
        """Content type set for this binary."""
        return self.contentType

    @property  # just for code completion
    def length(self) -> str:
        """Size of the binary file in bytes."""
        return super().__getattr__('length')

    def __repr__(self):
        return self._repr('name', 'type')

    @classmethod
    def from_json(cls, json: dict) -> Binary:
        """ Build a new Binary instance from JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Args:
            json (dict): JSON object (nested dictionary)
                representing a managed object within Cumulocity

        Returns:
            Binary object
        """
        return super()._from_json(json, Binary())

    def create(self) -> Binary:
        """ Create a new representation of this object within the database.

        This function can be called multiple times to create multiple
        instances of this object with different ID.

        Returns:
            A fresh Binary instance representing the created object
            within the database. This instance can be used to get
            at the ID of the new object.

        Raises:
            FileNotFoundError:  if the file refers to an invalid path.

        See also function `Binaries.create` which doesn't parse the result.
        """
        self._assert_c8y()
        response_json = self.c8y.post_file(self._build_resource_path(), file=self.file,
                                           object=self.to_full_json(), content_type=self.content_type)
        result = Binary.from_json(response_json)
        result.c8y = self.c8y
        return result

    def update(self) -> Binary:
        """Update the binary attachment.

        Returns:
            The Binary managed object.

        Returns:
            A fresh Binary instance representing the created object
            within the database.

        Raises:
            FileNotFoundError:  if the file refers to an invalid path.

        Note: The binary metadata cannot be updated using this method. Only
        the binary attachment is updated.
        """
        self._assert_c8y()
        response_json = self.c8y.put_file(self._build_object_path(), file=self.file, content_type=self.content_type)
        result = Binary.from_json(response_json)
        result.c8y = self.c8y
        return result

    def read_file(self) -> bytes:
        """Read the content of the binary attachment.

        Returns:
            The binary attachment's content as bytes.
        """
        self._assert_c8y()
        return Binaries(self.c8y).read_file(self.id)


class Binaries(CumulocityResource):
    """ Provides access to the Identity API.

    See also: https://cumulocity.com/api/#tag/Binaries
    """

    def __init__(self, c8y: CumulocityRestApi):
        super().__init__(c8y, 'inventory/binaries')
        self.object_name = 'managedObjects'

    def read_file(self, id: str) -> bytes:
        """Read the binary content of a specific binary.

        Args:
            id (str): The database ID of the binary object.

        Returns:
            The binary attachment's content as bytes
        """
        return self.c8y.get_file(self.build_object_path(id))

    def upload(self, file: str | BinaryIO, name: str, type: str) -> Binary:
        """Upload a file.

        Args:
            file (str | file-like):  File to upload
            name (str):  Virtual name of the file
            type (str):  Mimetype of the file

        Returns:
            A Binary instance referencing the uploaded file.

        Raises:
            FileNotFoundError:  if the file refers to an invalid path.
        """
        object_json = {
            'type': type,
            'name': name}
        return Binary.from_json(self.c8y.post_file(self.resource, file=file, object=object_json,
                                                   content_type=type))

    def create(self, *binaries: Binary):
        """Create binaries, i.e. upload files.

        Each of the binaries must have a file set. The binaries are created
        one by one, in case of an error the state is unclear.

        Args:
            *binaries (Binary):  Binaries to upload

        Returns:
            The number of successfully created binaries.

        Raises:
            FileNotFoundError:  if one of the file attributes within the
                binaries refers to an invalid file path
        """
        all_files = [b.file for b in binaries]
        n = 0
        for file, binary in zip(all_files, binaries):
            self.c8y.post_file(self.resource, file=file, object=binary.to_json(), accept='')
            n = n+1
        return n

    def update(self, id: str, file: str | BinaryIO, type: str = None):
        """Update a binary attachment.

        Args:
            id (str):  ID of an existing Binary within Cumulocity
            file (str|file-like):  File to upload
            type:  Content type of the file
                (defaults to 'application/octet-stream')

        Raises:
            FileNotFoundError:  if the file refers to an invalid path.
        """
        self.c8y.put_file(self.build_object_path(id), file=file, accept='', content_type=type)


    def select(
            self,
            ids: list[str | int] = None,
            type: str = None,
            owner: str = None,
            child_device: str | int = None,
            child_asset: str | int = None,
            child_addition: str | int = None,
            limit: int = None,
            include: str | JsonMatcher = None, exclude: str | JsonMatcher = None,
            page_size: int = 1000,
            page_number: int = None,
            as_values: str | tuple | list[str | tuple] = None,
            **kwargs) -> Generator[Binary]:
        """Query the database for binaries and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filter's specification.  Filters can be
        combined (within reason).

        Args:
            ids (List[str|int]): Specific object ID to select.
            type (str):  Event type
            owner (str):  Username of the object owner
            child_device (str|int): Object ID of a child device.
            child_asset (str|int): Object ID of a child asset.
            child_addition (str|int): Object ID of a child addition.
            limit (int): Limit the number of results to this number.
            include (str | JsonMatcher): Matcher/expression to filter the query
                results (on client side). The inclusion is applied first.
                Creates a PyDF (Python Display Filter) matcher by default for strings.
            exclude (str | JsonMatcher): Matcher/expression to filter the query
                results (on client side). The exclusion is applied second.
                Creates a PyDF (Python Display Filter) matcher by default for strings.
            page_size (int): Define the number of events which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.
            as_values: (*str|tuple):  Don't parse objects, but directly extract
                the values at certain JSON paths as tuples; If the path is not
                defined in a result, None is used; Specify a tuple to define
                a proper default value for each path.

        Returns:
            Generator for Binary instances
        """

        base_query = self._prepare_query(
            ids=ids,
            type=type,
            owner=owner,
            childDeviceId=child_device,
            childAssetId=child_asset,
            childAddition=child_addition,
            page_size=page_size,
            **kwargs)
        return super()._iterate(
            base_query,
            page_number,
            limit,
            include,
            exclude,
            Binary.from_json if not as_values else
            lambda x: as_tuple(x, as_values))

    def get_all(
            self,
            ids: list[str | int] = None,
            type: str = None,
            owner: str = None,
            child_device: str | int = None,
            child_asset: str | int = None,
            child_addition: str | int = None,
            limit: int = None,
            include: str | JsonMatcher = None, exclude: str | JsonMatcher = None,
            page_size: int = 1000,
            page_number: int = None,
            as_values: str | tuple | list[str | tuple] = None,
            **kwargs
    ) -> List[Binary]:

        """ Query the database for binary objects and return the results
        as list.

        This function is a greedy version of the `select` function. All
        available results are read immediately and returned as list.

        Returns:
            List of Binary instances
        """
        return list(self.select(
            ids=ids,
            type=type,
            owner=owner,
            child_device=child_device,
            child_asset=child_asset,
            child_addition=child_addition,
            limit=limit,
            include=include,
            exclude=exclude,
            page_size=page_size,
            page_number=page_number,
            as_values=as_values,
            **kwargs))

    def get_count(
            self,
            ids: list[str | int] = None,
            type: str = None,
            owner: str = None,
            child_device: str | int = None,
            child_asset: str | int = None,
            child_addition: str | int = None,
            **kwargs
        ) -> int:
        """Calculate the number of potential results of a database query.

        This function uses the same parameters as the `select` function.

        Returns:
            Number of potential results
        """
        base_query = self._prepare_query(
            ids=ids,
            type=type,
            owner=owner,
            childDeviceId=child_device,
            childAssetId=child_asset,
            childAddition=child_addition,
            **kwargs)
        return self._get_count(base_query)
