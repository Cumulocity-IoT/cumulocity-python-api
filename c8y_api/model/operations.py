# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

from typing import Type, List, Generator

from c8y_api._base_api import CumulocityRestApi

from c8y_api.model._base import CumulocityResource, ComplexObject
from c8y_api.model._parser import ComplexObjectParser
from c8y_api.model._base import _DictWrapper
from c8y_api.model._util import _DateUtil


class Operation(ComplexObject):
    """ Represents an instance of a operation object in Cumulocity.

    Instances of this class are returned by functions of the corresponding
    Operation API. Use this class to create new or update existing
    operation.

    See also: https://cumulocity.com/api/10.11.0/#tag/Operations
    """

    # these need to be defined like this for the abstract super functions
    _resource = '/devicecontrol/operations'
    _parser = ComplexObjectParser({'deviceId': 'deviceId', 'description': 'description'}, [])
    # _accept
    # _not_updatable

    def __init__(self, c8y=None, deviceId=None, description=None, **kwargs):
        """ Create a new Operation object.

        Params:
            c8y(CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            deviceId(str):  Device ID which this operation is for 
            kwargs:  All additional named arguments are interpreted as
                custom fragments e.g. for data points.

        Returns:
            Operation object
        """
        super().__init__(c8y, **kwargs)
        self.deviceId = deviceId
        self.description = description


    @classmethod
    def from_json(cls, json) -> Operation:
        """ Build a new Operation instance from Cumulocity JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Params:
            operation_json(dict):  JSON object (nested dictionary)
                representing a operation within Cumulocity

        Returns:
            Operation object
        """
        obj = cls._from_json(json, Operation())
        return obj

    def to_json(self, only_updated: bool = False) -> dict:
        # (no doc update required)
        # creation time is always excluded
        obj_json = super()._to_json(only_updated, exclude={'creation_time'})
        # source needs to be set manually, but it cannot be updated
        if not only_updated and self.deviceId:
            obj_json['deviceId'] = self.deviceId
        return obj_json

    # the __getattr__ function is overwritten to return a wrapper that doesn't signal updates
    # (because Measurements are not updated, can only be created from scratch)
    def __getattr__(self, item):
        return _DictWrapper(self.fragments[item], on_update=None)

    @property
    def datetime(self) -> Type[datetime] | None:
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

    def delete(self):
        """Delete the Operation within the database."""
        self._delete()


class Operations(CumulocityResource):
    """ A wrapper for the standard Operation API.

    This class can be used for get, search for, create, update and
    delete operations within the Cumulocity database.

    See also: https://cumulocity.com/api/10.11.0/#tag/Operations
    """

    def __init__(self, c8y: CumulocityRestApi):
        super().__init__(c8y, 'devicecontrol/operations')

    def get(self, operation_id: str | int) -> Operation:
        """ Read a specific operation from the database.

        params:
            operation_id (str|int):  database ID of a operation

        Returns:
            Operation object

        Raises:
            KeyError: If the ID cannot be resolved.
        """
        operation = Operation.from_json(self._get_object(operation_id))
        operation.c8y = self.c8y  # inject c8y connection into instance
        return operation

    def select(self, agentId=None, deviceId=None, status=None, fragment=None,
               before=None, after=None, min_age=None, max_age=None, reverse=False,
               limit=None, page_size=1000) -> Generator[Operation]:
        """ Query the database for operations and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification.  Filters can be
        combined (within reason).

        Params:
            agentId (str): Database ID of agent
            deviceId (str):  Database ID of device
            status (str): Status of operation
            fragment (str):  Name of a present custom/standard fragment
            before (datetime|str):  Datetime object or ISO date/time string.
                Only operaton assigned to a time before this date are
                returned.
            after (datetime|str):  Datetime object or ISO date/time string.
                Only operation assigned to a time after this date are
                returned.
            min_age (timedelta):  Timedelta object. Only operation of
                at least this age are returned.
            max_age (timedelta):  Timedelta object. Only operations with
                at most this age are returned.
            reverse (bool):  Invert the order of results, starting with the
                most recent one.
            limit (int):  Limit the number of results to this number.
            page_size (int):  Define the number of operations which are
                read (and parsed in one chunk). This is a performance
                related setting.

        Returns:
            Generator[Operation]: Iterable of matching Operation objects
        """
        base_query = self._build_base_query(agentId=agentId, deviceId=deviceId, status=status, fragment=fragment,
                                            before=before, after=after, min_age=min_age, max_age=max_age,
                                            reverse=reverse, page_size=page_size)
        return super()._iterate(base_query, limit, Operation.from_json)

    def get_all(self, agentId=None, deviceId=None, status=None,  # noqa (type)
                fragment=None,  value=None, series=None,
                before=None, after=None, min_age=None, max_age=None, reverse=False,
                limit=None, page_size=1000) -> List[Operation]:
        """ Query the database for operations and return the results
        as list.

        This function is a greedy version of the select function. All
        available results are read immediately and returned as list.

        Returns:
            List of matching Measurement objects
        """
        return list(self.select(agentId=agentId, deviceId=deviceId, status=status,
                                fragment=fragment, before=before, after=after, min_age=min_age, max_age=max_age,
                                reverse=reverse, limit=limit, page_size=page_size))

    def get_last(self, agentId=None, deviceId=None, status=None, fragment=None, value=None, series=None,  # noqa (type)
                 before=None, min_age=None) -> Operation:
        """ Query the database and return the last matching operation.

        This function is a special variant of the select function. Only
        the last matching result is returned.

        Returns:
            Last matching Operation object
        """
        # at least one date qualifier is required for this query to function,
        # so we enforce the 'after' filter if nothing else is specified
        after = None
        if not before and not min_age:
            after = '1970-01-01'
        base_query = self._build_base_query(agentId=agentId, deviceId=deviceId, status=status,
                                            fragment=fragment, after=after,
                                            before=before, min_age=min_age, reverse=True, page_size=1)
        m = Operation.from_json(self._get_page(base_query, "1")[0])
        m.c8y = self.c8y  # inject c8y connection into instance
        return m

