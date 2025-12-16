# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Generator

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model.matcher import JsonMatcher
from c8y_api.model._base import (
    CumulocityResource,
    ComplexObject,
    SimpleObject,
    _DictWrapper,
    sanitize_page_size,
    as_tuple,
)
from c8y_api.model._parser import ComplexObjectParser
from c8y_api.model._util import _DateUtil


class Operation(ComplexObject):
    """ Represents an instance of an operation object in Cumulocity.

    Instances of this class are returned by functions of the corresponding
    Operation API. Use this class to create new or update existing
    operation.

    See also: https://cumulocity.com/api/core/#tag/Operations
    """

    class Status:
        """Operation statuses."""
        PENDING = 'PENDING'
        EXECUTING = 'EXECUTING'
        SUCCESSFUL = 'SUCCESSFUL'
        FAILED = 'FAILED'

    # these need to be defined like this for the abstract super functions
    _resource = '/devicecontrol/operations'
    _parser = ComplexObjectParser({
        'device_id': 'deviceId',
        'creation_time': 'creationTime',
        '_u_description': 'description',
        '_u_status': 'status'}, [])
    _accept = 'application/vnd.com.nsn.cumulocity.operation+json'

    def __init__(self, c8y=None, device_id=None, description=None, status=None, **kwargs):
        """ Create a new Operation object.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            device_id (str):  Device ID which this operation is for
            kwargs:  All additional named arguments are interpreted as
                custom fragments e.g. for data points.

        Returns:
            Operation object
        """
        super().__init__(c8y, **kwargs)
        self.device_id = device_id
        self.creation_time = None
        self._u_description = description
        self._u_status = status

    description = SimpleObject.UpdatableProperty('_u_description')
    status = SimpleObject.UpdatableProperty('_u_status')

    @property
    def creation_datetime(self) -> datetime:
        """Convert the operation's creation time to a Python datetime object.

        Returns:
            Standard Python datetime object for the operation's creation time.
        """
        return super()._to_datetime(self.creation_time)

    def __repr__(self):
        return self._repr('device_id', 'status', 'description')

    @classmethod
    def from_json(cls, json) -> Operation:
        """ Build a new Operation instance from Cumulocity JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Args:
            json (dict):  JSON object (nested dictionary)
                representing an operation within Cumulocity

        Returns:
            Operation object
        """
        obj = cls._from_json(json, Operation())
        return obj

    def to_json(self, only_updated: bool = False) -> dict:
        # (no doc update required)
        # creation time is always excluded
        obj_json = super()._to_json(only_updated, exclude={'creation_time'})
        return obj_json

    # # the __getattr__ function is overwritten to return a wrapper that doesn't signal updates
    # # (because Measurements are not updated, can only be created from scratch)
    # def __getattr__(self, item):
    #     return _DictWrapper(self.fragments[item], on_update=None)
    #
    @property
    def datetime(self) -> datetime | None:
        """ Convert the measurement's time to a Python datetime object.

        Returns:
            (datetime): The measurement's time
        """
        if self.time:
            return _DateUtil.to_datetime(self.time)
        return None

    def create(self) -> Operation:
        """ Store the Operation within the database.

        Returns:  A fresh Operation object representing what was
            created within the database (including the ID).
        """
        return self._create()

    def update(self) -> Operation:
        """Update the Operation within the database."""
        return super()._update()


class Operations(CumulocityResource):
    """ A wrapper for the standard Operation API.

    This class can be used for get, search for, create, update and
    delete operations within the Cumulocity database.

    See also: https://cumulocity.com/api/core/#tag/Operations
    """

    def __init__(self, c8y: CumulocityRestApi):
        super().__init__(c8y, 'devicecontrol/operations')

    def get(self, operation_id: str | int) -> Operation:
        """ Read a specific operation from the database.

        Args:
            operation_id (str|int):  database ID of an operation

        Returns:
            Operation object

        Raises:
            KeyError:  if the ID cannot be resolved.
        """
        operation = Operation.from_json(self._get_object(operation_id))
        operation.c8y = self.c8y  # inject c8y connection into instance
        return operation

    def select(
            self,
            expression: str = None,
            agent_id: str = None, device_id: str = None, status: str = None,
            bulk_id: str = None, fragment: str = None,
            before: str | datetime = None, after: str | datetime = None,
            min_age: timedelta = None, max_age: timedelta = None,
            date_from: str | datetime = None, date_to: str | datetime = None,
            reverse: bool = False, limit: int = None,
            include: str | JsonMatcher = None, exclude: str | JsonMatcher = None,
            page_size: int = 1000, page_number: int = None,
            as_values: str | tuple | list[str | tuple] = None,
            **kwargs
    ) -> Generator[Operation]:
        """ Query the database for operations and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification.  Filters can be
        combined (within reason).

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            agent_id (str): Database ID of agent
            device_id (str):  Database ID of device
            status (str): Status of operation
            bulk_id (str): The bulk operation ID that this object belongs to
            fragment (str):  Name of a present custom/standard fragment
            before (datetime|str):  Datetime object or ISO date/time string.
                Only operations assigned to a time before this date are
                returned.
            after (datetime|str):  Datetime object or ISO date/time string.
                Only operations assigned to a time after this date are
                returned.
            min_age (timedelta):  Timedelta object. Only operation of
                at least this age are returned.
            max_age (timedelta):  Timedelta object. Only operations with
                at most this age are returned.
            date_from (str|datetime): Same as `after`
            date_to (str|datetime): Same as `before`
            reverse (bool):  Invert the order of results, starting with the
                most recent one.
            limit (int):  Limit the number of results to this number.
            include (str | JsonMatcher): Matcher/expression to filter the query
                results (on client side). The inclusion is applied first.
                Creates a PyDF (Python Display Filter) matcher by default for strings.
            exclude (str | JsonMatcher): Matcher/expression to filter the query
                results (on client side). The exclusion is applied second.
                Creates a PyDF (Python Display Filter) matcher by default for strings.
            page_size (int):  Define the number of operations which are
                read (and parsed in one chunk). This is a performance
                related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.
            as_values: (*str|tuple):  Don't parse objects, but directly extract
                the values at certain JSON paths as tuples; If the path is not
                defined in a result, None is used; Specify a tuple to define
                a proper default value for each path.

        Returns:
            Generator[Operation]: Iterable of matching Operation objects

        See also:
            https://github.com/bytebutcher/pydfql/blob/main/docs/USER_GUIDE.md#4-query-language
        """
        base_query = self._prepare_query(
            expression=expression,
            agent_id=agent_id, device_id=device_id, status=status, bulk_id=bulk_id,
            fragment=fragment,
            before=before, after=after,
            min_age=min_age, max_age=max_age,
            date_from=date_from, date_to=date_to,
            reverse=reverse, page_size=sanitize_page_size(limit, page_size),
            **kwargs
        )
        return super()._iterate(
            base_query,
            page_number,
            limit,
            include,
            exclude,
            Operation.from_json if not as_values else
            lambda x: as_tuple(x, as_values))

    def get_all(
            self,
            expression: str = None,
            agent_id: str = None, device_id: str = None, status: str = None,
            bulk_id: str = None, fragment: str = None,
            before: str | datetime = None, after: str | datetime = None,
            min_age: timedelta = None, max_age: timedelta = None,
            date_from: str | datetime = None, date_to: str | datetime = None,
            reverse: bool = False, limit: int = None,
            include: str | JsonMatcher = None, exclude: str | JsonMatcher = None,
            page_size: int = 1000, page_number: int = None,
            as_values: str | tuple | list[str | tuple] = None,
            **kwargs
    ) -> List[Operation]:
        """ Query the database for operations and return the results
        as list.

        This function is a greedy version of the select function. All
        available results are read immediately and returned as list.

        Returns:
            List of matching Operation objects
        """
        return list(self.select(
            expression=expression,
            agent_id=agent_id, device_id=device_id, status=status, bulk_id=bulk_id,
            fragment=fragment,
            before=before, after=after,
            min_age=min_age, max_age=max_age,
            date_from=date_from, date_to=date_to,
            reverse=reverse,
            limit=limit,
            include=include, exclude=exclude,
            page_size=page_size,
            page_number=page_number,
            as_values=as_values,
            **kwargs
        ))

    def get_last(
            self,
            expression: str = None,
            agent_id: str = None, device_id: str = None, status: str = None,
            bulk_id: str = None, fragment: str = None,
            date_to: str | datetime = None,
            before: str | datetime = None,
            min_age: timedelta = None,
            **kwargs
    ) -> Operation|None:
        """ Query the database and return the last matching operation.

        This function is a special variant of the select function. Only
        the last matching result is returned.

        Returns:
            Last matching Operation object or None
        """
        # at least one date qualifier is required for this query to function,
        # so we enforce the 'after' filter if nothing else is specified
        after = None
        if not before and not min_age:
            after = '1970-01-01'
        base_query = self._prepare_query(
            expression=expression,
            agent_id=agent_id, device_id=device_id, status=status,
            bulk_id=bulk_id, fragment=fragment, after=after,
            date_to=date_to, before=before, min_age=min_age,
            reverse=True, page_size=1,
            **kwargs
        )
        r = self._get_page(base_query, 1)
        if not r:
            return None
        o = Operation.from_json(r[0])
        o.c8y = self.c8y  # inject c8y connection into instance
        return o

    def get_count(
            self,
            expression: str = None,
            agent_id: str = None, device_id: str = None, status: str = None,
            bulk_id: str = None, fragment: str = None,
            before: str | datetime = None, after: str | datetime = None,
            min_age: timedelta = None, max_age: timedelta = None,
            date_from: str | datetime = None, date_to: str | datetime = None,
            **kwargs
    ) -> int:
        """Calculate the number of potential results of a database query.

        This function uses the same parameters as the `select` function.

        Returns:
            Number of potential results
        """
        base_query = self._prepare_query(
            expression=expression,
            agent_id=agent_id, device_id=device_id, status=status,
            bulk_id=bulk_id, fragment=fragment,
            date_from=date_from, date_to=date_to,
            before=before, after=after,
            min_age=min_age, max_age=max_age,
            **kwargs
        )
        return self._get_count(base_query)

    def delete_by(
            self,
            expression: str = None,
            agent_id: str = None, device_id: str = None, status: str = None,
            bulk_id: str = None, fragment: str = None,
            before: str | datetime = None, after: str | datetime = None,
            min_age: timedelta = None, max_age: timedelta = None,
            **kwargs
    ):
        """Query the database and delete matching operations.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification. Filters can be
        combined (as defined in the Cumulocity REST API).

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            agent_id (str): Database ID of agent
            device_id (str):  Database ID of device
            status (str): Status of operation
            bulk_id (str): The bulk operation ID that this object belongs to
            fragment (str):  Name of a present custom/standard fragment
            before (datetime|str):  Datetime object or ISO date/time string.
                Only operations assigned to a time before this date are
                selected.
            after (datetime|str):  Datetime object or ISO date/time string.
                Only operation assigned to a time after this date are
                selected.
            min_age (timedelta):  Timedelta object. Only operation of
                at least this age are returned.
            max_age (timedelta):  Timedelta object. Only operations with
                at most this age are returned.
                # build a base query
        """
        base_query = self._prepare_query(
            expression=expression,
            agent_id=agent_id, device_id=device_id, status=status,
            bulk_id=bulk_id, fragment=fragment,
            before=before, after=after, min_age=min_age, max_age=max_age,
            **kwargs
        )
        self.c8y.delete(base_query)


class BulkOperation(ComplexObject):
    """ Represents an instance of a bulk operation object in Cumulocity.

    Instances of this class are returned by functions of the corresponding
    Bulk Operation API. Use this class to create new or update existing
    operation.

    See also: https://cumulocity.com/api/core/#tag/Bulk-operations
    """

    class Status:
        """Bulk Operation statuses."""
        ACTIVE = 'ACTIVE'
        IN_PROGRESS = 'IN_PROGRESS'
        COMPLETED = 'COMPLETED'
        DELETED = 'DELETED'

    class GeneralStatus:
        """Bulk Operation general statuses."""
        SCHEDULED = 'PENDING'
        EXECUTING = 'EXECUTING'
        EXECUTING_WITH_ERRORS = 'EXECUTING_WITH_ERRORS'
        SUCCESSFUL = 'SUCCESSFUL'
        FAILED = 'FAILED'
        CANCELED = 'CANCELED'
        COMPLETED_SUCCESSFULLY = 'COMPLETED SUCCESSFULLY'
        COMPLETED_WITH_FAILURES = 'COMPLETED WITH FAILURES'

    # these need to be defined like this for the abstract super functions
    _resource = '/devicecontrol/bulkoperations'
    _parser = ComplexObjectParser({
        '_u_group_id': 'groupId',
        '_u_failed_parent_id': 'failedParentId',
        '_u_start_time': 'startDate',
        '_u_creation_ramp': 'creationRamp',
        'status': 'status',
        'general_status': 'generalStatus'}, [])
    _accept = 'application/vnd.com.nsn.cumulocity.bulkoperation+json'

    def __init__(self, c8y=None, group_id: str = None, failed_parent_id: str = None,
                 start_time: str | datetime = None, creation_ramp: float = None,
                 operation_prototype: dict = None, **kwargs):
        """ Create a new Operation object.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            device_id (str):  Device ID which this operation is for
            kwargs:  All additional named arguments are interpreted as
                custom fragments e.g. for data points.

        Returns:
            Operation object
        """
        super().__init__(c8y, operationPrototype=operation_prototype, **kwargs)
        self._u_group_id = group_id
        self._u_failed_parent_id = failed_parent_id
        self._u_start_time = _DateUtil.ensure_timestring(start_time)
        self._u_creation_ramp = creation_ramp
        self.status: str | None = None
        self.general_status: str | None = None

    group_id = SimpleObject.UpdatableProperty('_u_group_id')
    failed_parent_id = SimpleObject.UpdatableProperty('_u_failed_parent_id')
    start_time = SimpleObject.UpdatableProperty('_u_start_time')
    creation_ramp = SimpleObject.UpdatableProperty('_u_creation_ramp')

    @property
    def operation_prototype(self) -> _DictWrapper:
        # pylint: disable=missing-function-docstring
        return self.operationPrototype

    @operation_prototype.setter
    def operation_prototype(self, fragment):
        self['operationPrototype'] = fragment

    @property
    def start_datetime(self) -> datetime:
        """Convert the operation's start time to a Python datetime object.

        Returns:
            Standard Python datetime object for the operation's start time.
        """
        return super()._to_datetime(self._u_start_time)

    @classmethod
    def from_json(cls, json) -> BulkOperation:
        """ Build a new Operation instance from Cumulocity JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Args:
            json (dict):  JSON object (nested dictionary)
                representing an operation within Cumulocity

        Returns:
            Operation object
        """
        obj = cls._from_json(json, BulkOperation())
        return obj

    def create(self) -> BulkOperation:
        """ Store the Bulk Operation within the database.

        Returns:  A fresh BulkOperation object representing what was
            created within the database (including the ID).
        """
        return self._create()

    def update(self) -> BulkOperation:
        """Update the BulkOperation within the database.

        Returns:  A fresh BulkOperation object representing the updated
            object within the database (including the ID).
        """
        return super()._update()


class BulkOperations(CumulocityResource):
    """ A wrapper for the standard Bulk Operation API.

    This class can be used for get, search for, create, update and
    delete bulk operations within the Cumulocity database.

    See also: https://cumulocity.com/api/core/#tag/Bulk-operations
    """

    def __init__(self, c8y: CumulocityRestApi):
        # bulk operations don't follow standard naming conventions
        super().__init__(c8y, 'devicecontrol/bulkoperations')
        self.object_name = 'bulkOperations'

    def get(self, operation_id: str | int) -> BulkOperation:
        """ Read a specific bulk operation from the database.

        Args:
            operation_id (str|int):  database ID of a bulk operation

        Returns:
            BulkOperation object

        Raises:
            KeyError:  if the ID cannot be resolved.
        """
        operation = BulkOperation.from_json(self._get_object(operation_id))
        operation.c8y = self.c8y  # inject c8y connection into instance
        return operation

    def select(self,
               limit: int = None,
               include: str | JsonMatcher = None,
               exclude: str | JsonMatcher = None,
               page_size: int = 1000,
               page_number: int = None,
               **kwargs) -> Generator[BulkOperation]:
        """ Query the database for operations and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters' specification.  Filters can be
        combined (within reason).

        Args:
            limit (int):  Limit the number of results to this number.
            include (str | JsonMatcher): Matcher/expression to filter the query
                results (on client side). The inclusion is applied first.
                Creates a PyDF (Python Display Filter) matcher by default for strings.
            exclude (str | JsonMatcher): Matcher/expression to filter the query
                results (on client side). The exclusion is applied second.
                Creates a PyDF (Python Display Filter) matcher by default for strings.
            page_size (int):  Define the number of operations which are
                read (and parsed in one chunk). This is a performance
                related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator[BulkOperation]: Iterable of matching BulkOperation objects
        """
        base_query = self._prepare_query(page_size=sanitize_page_size(limit, page_size), **kwargs)
        return super()._iterate(base_query, page_number, limit, include, exclude, BulkOperation.from_json)

    def get_all(self,
                limit: int = None,
                include: str | JsonMatcher = None,
                exclude: str | JsonMatcher = None,
                page_size: int = 1000,
                page_number: int = None,
                **kwargs) -> List[BulkOperation]:
        """ Query the database for bulk operations and return the results
        as list.

        This function is a greedy version of the select function. All
        available results are read immediately and returned as list.

        Returns:
            List of matching BulkOperation objects
        """
        return list(self.select(
            include=include,
            exclude=exclude,
            limit=limit,
            page_size=page_size,
            page_number=page_number,
            **kwargs))

    def get_count(self, **kwargs) -> int:
        """Calculate the number of potential results of a database query.

        This function uses the same parameters as the `select` function.

        Returns:
            Number of potential results
        """
        return self._get_count(self._prepare_query(**kwargs))
