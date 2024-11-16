# Copyright (c) 2021 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Generator

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model._base import CumulocityResource, SimpleObject, ComplexObject
from c8y_api.model._parser import ComplexObjectParser
from c8y_api.model._util import _DateUtil


class Alarm(ComplexObject):
    """Represent an instance of an alarm object in Cumulocity.

    Instances of this class are returned by functions of the corresponding
    Alarms API. Use this class to create new or update Alarm objects.

    See also: https://cumulocity.com/api/#tag/Alarms
    """

    class Severity:
        """Alarm severity levels."""
        MAJOR = 'MAJOR'
        CRITICAL = 'CRITICAL'
        MINOR = 'MINOR'
        WARNING = 'WARNING'

    class Status:
        """Alarm statuses."""
        ACTIVE = 'ACTIVE'
        ACKNOWLEDGED = 'ACKNOWLEDGED'
        CLEARED = 'CLEARED'

    _resource = '/alarm/alarms/'
    _parser = ComplexObjectParser({
        'id': 'id',
        'type': 'type',
        'time': 'time',
        '_u_text': 'text',
        # 'source': 'source/id'   - cannot be parsed automatically
        'creation_time': 'creationTime',
        'updated_time': 'lastUpdated',
        '_u_status': 'status',
        '_u_severity': 'severity',
        'count': 'count',
        'first_occurrence': 'firstOccurrenceTime'},
        ['source'])

    def __init__(self, c8y: CumulocityRestApi = None, type: str = None, time: str | datetime = None,  # noqa (type)
                 source: str = None, text: str = None, status: str = None, severity: str = None, **kwargs):
        """Create a new Alarm object.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            type (str):  Alarm type
            time (str|datetime):  Date/time of the alarm. Can be provided as
                timezone-aware datetime object or formatted string (in
                standard ISO format incl. timezone: YYYY-MM-DD'T'HH:MM:SS.SSSZ
                as it is returned by the Cumulocity REST API).
                Use 'now' to set  to current datetime in UTC.
            source (str):  ID of the device which this Alarm is raised by
            text (str):  Alarm test/description
            status (str):  Alarm status
            severity (str):  Alarm severity
            kwargs:  Additional arguments are treated as custom fragments
        """
        super().__init__(c8y=c8y, **kwargs)
        self.type = type
        self.source = source
        self._u_text = text
        self.creation_time = None
        self.updated_time = None
        self._u_status = status
        self._u_severity = severity
        self.count = None
        self.first_occurrence_time = None
        # The time can either be set as string (e.g. when read from JSON) or
        # as a datetime object. It will be converted to string immediately
        # as there is no scenario where a manually created object won't be
        # written to Cumulocity anyway
        self.time = _DateUtil.ensure_timestring(time)

    text = SimpleObject.UpdatableProperty('_u_text')
    """Updatable property"""
    status = SimpleObject.UpdatableProperty('_u_status')
    severity = SimpleObject.UpdatableProperty('_u_severity')

    @property
    def datetime(self) -> datetime:
        """Convert the alarm's time to a Python datetime object.

        Returns:
            Standard Python datetime object for the alarm's time.
        """
        return super()._to_datetime(self.time)

    @property
    def creation_datetime(self) -> datetime:
        """Convert the alarm's creation time to a Python datetime object.

        Returns:
            Standard Python datetime object for the alarm's creation time.
        """
        return super()._to_datetime(self.creation_time)

    @property
    def updated_datetime(self) -> datetime:
        """Convert the alarm's last updated time to a Python datetime object.

        Returns:
            Standard Python datetime object for the alarm's last updated time.
        """
        return super()._to_datetime(self.updated_time)

    @property
    def first_occurrence_datetime(self) -> datetime:
        """Convert the first occurrence time to a Python datetime object.

        Returns:
            Standard Python datetime object for the first occurrence time.
        """
        return super()._to_datetime(self.first_occurrence_time)

    @classmethod
    def from_json(cls, json: dict) -> Alarm:
        """Build a new Alarm instance from JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Args:
            json (dict): JSON object (nested dictionary)
                representing a managed object within Cumulocity

        Returns:
            Alarm object
        """
        obj = super()._from_json(json, Alarm())
        obj.source = json['source']['id']
        obj.id = json['id']
        return obj

    def to_json(self, only_updated=False) -> dict:
        """Convert the instance to JSON.

        The JSON format produced by this function is what is used by the
        Cumulocity REST API.

        Args:
            only_updated (bool):  Whether only updated fields should be
                included in the generated JSON

        Returns:
            JSON object (nested dictionary)
        """
        alarm_json = self._to_json(only_updated)
        if self.source:
            alarm_json['source'] = {'id': self.source}
        return alarm_json

    def create(self) -> Alarm:
        """Create the Alarm within the database.

        Returns:
            A fresh Alarm instance representing the created object
            within the database. This instance can be used to get at the ID
            of the new Alarm object.
        """
        return super()._create()

    def update(self) -> Alarm:
        """Update the Alarm within the database.

        Note: This will only send changed fields to increase performance.

        Returns:
            A fresh Alarm instance representing the updated object
            within the database.
        """
        return super()._update()

    def apply_to(self, other_id: str) -> Alarm:
        """Apply changes made to this object to another object in the database.

        Args:
            other_id (str): Database ID of the Alarm to update.

        Returns:
            A fresh Alarm instance representing the updated object
            within the database.
        """
        return super()._apply_to(other_id)

    def delete(self):
        """Delete this object within the database.

        An alarm is identified through its type and source. These fields
        must be defined for this to function. This is always the case if
        the instance was built by the API.

        See also functions `Alarms.delete` and `Alarms.delete_by`
        """
        self._assert_c8y()
        if not self.type:
            raise ValueError("The alarm type must be set to allow unambiguous identification.")
        if not self.source:
            raise ValueError("The alarm source must be set to allow unambiguous identification.")
        Alarms(self.c8y).delete_by(type=self.type, source=self.source)


class Alarms(CumulocityResource):
    """A wrapper for the standard Alarms API.

    This class can be used for get, search for, create, update and
    delete alarms within the Cumulocity database.

    See also: https://cumulocity.com/api/#tag/Alarms
    """

    _RESOURCE = '/alarm/alarms/'

    def __init__(self, c8y):
        super().__init__(c8y, self._RESOURCE)

    def get(self, id: str) -> Alarm:  # noqa (id)
        """Retrieve a specific object from the database.

        Args:
            id (str): The database ID of the object

        Returns:
            An Alarm instance representing the object in the database.
        """
        obj = Alarm.from_json(self._get_object(id))
        obj.c8y = self.c8y  # inject c8y connection into instance
        return obj

    def select(self,
               expression: str = None,
               type: str = None, source: str = None, fragment: str = None, # noqa (type)
               status: str = None, severity: str = None, resolved: str = None,
               before: str | datetime = None, after: str | datetime = None,
               date_from: str | datetime = None, date_to: str | datetime = None,
               created_before: str | datetime = None, created_after: str | datetime = None,
               created_from: str | datetime = None, created_to: str | datetime = None,
               updated_before: str | datetime = None, updated_after: str | datetime = None,
               last_updated_from: str | datetime = None, last_updated_to: str | datetime = None,
               min_age: timedelta = None, max_age: timedelta = None,
               reverse: bool = False, limit: int = None,
               with_source_assets: bool = None, with_source_devices: bool = None,
               page_size: int = 1000, page_number: int = None,
               **kwargs) -> Generator[Alarm]:
        """Query the database for alarms and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification.  Filters can be
        combined (as defined in the Cumulocity REST API).

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            type (str):  Alarm type
            source (str):  Database ID of a source device
            fragment (str):  Name of a present custom/standard fragment
            status (str):  Alarm status
            severity (str):  Alarm severity
            resolved (str):  Whether the alarm status is CLEARED
            before (str|datetime):  Datetime object or ISO date/time string.
                Only alarms assigned to a time before this date are returned.
            after (str|datetime):  Datetime object or ISO date/time string.
                Only alarms assigned to a time after this date are returned
            created_before (str|datetime):  Datetime object or ISO date/time string.
                Only alarms changed at a time before this date are returned.
            created_after (str|datetime):  Datetime object or ISO date/time string.
                Only alarms changed at a time after this date are returned.
            updated_before (str|datetime):  Datetime object or ISO date/time string.
                Only alarms changed at a time before this date are returned.
            updated_after (str|datetime):  Datetime object or ISO date/time string.
                Only alarms changed at a time after this date are returned.
            min_age (timedelta):  Matches only alarms of at least this age
            max_age (timedelta):  Matches only alarms with at most this age
            date_from (str|datetime): Same as `after`
            date_to (str|datetime): Same as `before`
            created_from (str|datetime): Same as `created_after`
            created_to(str|datetime): Same as `created_before`
            last_updated_from (str|datetime): Same as `updated_after`
            last_updated_to (str|datetime): Same as `updated_before`
            with_source_assets (bool): Whether also alarms for related source
                assets should be included. Requires `source`.
            with_source_devices (bool): Whether also alarms for related source
                devices should be included. Requires `source`
            reverse (bool):  Invert the order of results, starting with the
                most recent one
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of alarms which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator of Alarm objects
        """
        base_query = self._prepare_query(
            expression=expression,
            type=type, source=source, fragment=fragment,
            status=status, severity=severity, resolved=resolved,
            before=before, after=after,
            date_from=date_from, date_to=date_to,
            created_before=created_before, created_after=created_after,
            created_from=created_from, created_to=created_to,
            updated_before=updated_before, updated_after=updated_after,
            last_updated_from=last_updated_from, last_updated_to=last_updated_to,
            with_source_devices=with_source_devices, with_source_assets=with_source_assets,
            min_age=min_age, max_age=max_age,
            reverse=reverse, page_size=page_size,
            **kwargs)
        return super()._iterate(base_query, page_number, limit, Alarm.from_json)

    def get_all(
            self,
            expression: str = None,
            type: str = None, source: str = None, fragment: str = None,  # noqa (type)
            status: str = None, severity: str = None, resolved: str = None,
            before: str | datetime = None, after: str | datetime = None,
            date_from: str | datetime = None, date_to: str | datetime = None,
            created_before: str | datetime = None, created_after: str | datetime = None,
            created_from: str | datetime = None, created_to: str | datetime = None,
            updated_before: str | datetime = None, updated_after: str | datetime = None,
            last_updated_from: str | datetime = None, last_updated_to: str | datetime = None,
            min_age: timedelta = None, max_age: timedelta = None,
            with_source_assets: bool = None, with_source_devices: bool = None,
            reverse: bool = False, limit: int = None,
            page_size: int = 1000, page_number: int = None,
            **kwargs) -> List[Alarm]:
        """Query the database for alarms and return the results as list.

        This function is a greedy version of the select function. All
        available results are read immediately and returned as list.

        See `select` for a documentation of arguments.

        Returns:
            List of Alarm objects
        """
        return list(self.select(
            expression=expression,
            type=type, source=source, fragment=fragment,
            status=status, severity=severity, resolved=resolved,
            before=before, after=after,
            date_from=date_from, date_to=date_to,
            created_before=created_before, created_after=created_after,
            created_from=created_from, created_to=created_to,
            updated_before=updated_before, updated_after=updated_after,
            last_updated_from=last_updated_from, last_updated_to=last_updated_to,
            min_age=min_age, max_age=max_age, reverse=reverse,
            with_source_devices=with_source_devices, with_source_assets=with_source_assets,
            limit=limit, page_size=page_size, page_number=page_number,
            **kwargs))

    def count(
            self,
            expression: str = None,
            type: str = None, source: str = None, fragment: str = None,  # noqa (type)
            status: str = None, severity: str = None, resolved: str = None,
            date_from: str | datetime = None, date_to: str | datetime = None,
            before: str | datetime = None, after: str | datetime = None,
            min_age: timedelta = None, max_age: timedelta = None,
            with_source_assets: bool = None, with_source_devices: bool = None,
            **kwargs) -> int:
        """Count the number of certain alarms.

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            type (str):  Alarm type
            source (str):  Database ID of a source device
            fragment (str):  Name of a present custom/standard fragment
            status (str):  Alarm status
            severity (str):  Alarm severity
            resolved (str):  Whether the alarm status is CLEARED
            before (str|datetime):  Datetime object or ISO date/time string.
                Only alarms assigned to a time before this date are returned.
            after (str|datetime):  Datetime object or ISO date/time string.
                Only alarms assigned to a time after this date are returned
            date_from (str|datetime): Same as `after`
            date_to (str|datetime): Same as `before`
            min_age (timedelta):  Matches only alarms of at least this age
            max_age (timedelta):  Matches only alarms with at most this age
            with_source_assets (bool): Whether also alarms for related source
                assets should be included. Requires `source`.
            with_source_devices (bool): Whether also alarms for related source
                devices should be included. Requires `source`

        Returns:
            Number of matching alarms in Cumulocity.
        """
        base_query = self._prepare_query(
            resource=f'{self.resource}/count',
            expression=expression,
            type=type, source=source, fragment=fragment,
            status=status, severity=severity, resolved=resolved,
            before=before, after=after, min_age=min_age, max_age=max_age,
            date_from=date_from, date_to=date_to,
            with_source_devices=with_source_devices, with_source_assets=with_source_assets,
            **kwargs)
        response_json = self.c8y.get(base_query)
        return response_json if isinstance(response_json, int) else None

    def create(self, *alarms):
        """Create alarm objects within the database.

        Args:
            *alarms (Alarm): Collection of Alarm instances
        """
        super()._create(Alarm.to_full_json, *alarms)

    def update(self, *alarms):
        """Write changes to the database.

        Args:
            *alarms (Alarm): Collection of Alarm instances
        """
        super()._update(Alarm.to_diff_json, *alarms)

    def apply_to(self, alarm: Alarm|dict, *alarm_ids: str):
        """Apply changes made to a single instance to other objects in the database.

        Args:
            alarm (Alarm|dict): Object serving as model for the update or
                simply a dictionary representing the diff JSON.
            *alarm_ids (str): A collection of database IDS of alarms
        """
        super()._apply_to(Alarm.to_full_json, alarm, *alarm_ids)

    def apply_by(
            self,
            alarm: Alarm,
            expression: str = None,
            type: str = None, source: str = None, fragment: str = None,
            status: str = None, severity: str = None, resolved: str = None,
            date_from: str | datetime = None, date_to: str | datetime = None,
            before: str | datetime = None, after: str | datetime = None,
            min_age: timedelta = None, max_age: timedelta = None,
            with_source_assets: bool = None, with_source_devices: bool = None,
            **kwargs):
        """Apply changes made to a single instance to other objects in the database.

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            alarm (Alarm): Object serving as model for the update
            type (str):  Alarm type
            source (str):  Database ID of a source device
            fragment (str):  Name of a present custom/standard fragment
            status (str):  Alarm status
            severity (str):  Alarm severity
            resolved (str):  Whether the alarm status is CLEARED
            before (str|datetime):  Datetime object or ISO date/time string.
                Only alarms assigned to a time before this date are returned.
            after (str|datetime):  Datetime object or ISO date/time string.
                Only alarms assigned to a time after this date are returned
            date_from (str|datetime): Same as `after`
            date_to (str|datetime): Same as `before`
            min_age (timedelta):  Matches only alarms of at least this age
            max_age (timedelta):  Matches only alarms with at most this age
            with_source_assets (bool): Whether also alarms for related source
                assets should be included. Requires `source`.
            with_source_devices (bool): Whether also alarms for related source
                devices should be included. Requires `source`

        See also: https://cumulocity.com/api/#operation/putAlarmCollectionResource
        """
        base_query = self._prepare_query(
            expression=expression,
            type=type, source=source, fragment=fragment,
            status=status, severity=severity, resolved=resolved,
            before=before, after=after, min_age=min_age, max_age=max_age,
            date_from=date_from, date_to=date_to,
            with_source_devices=with_source_devices, with_source_assets=with_source_assets,
            **kwargs)
        self.c8y.put(base_query, alarm.to_full_json(), accept='')

    def delete(self, *alarms):
        """Delete alarm objects within the database.

        Note: within Cumulocity alarms are identified by type and source.
        These fields must be defined within the provided objects for this
        operation to function.

        Args:
            *alarms (Alarm): Collection of Alarm instances.
        """
        for a in alarms:
            a.delete()

    def delete_by(
            self,
            expression: str = None,
            type: str = None, source: str = None, fragment: str = None,
            status: str = None, severity: str = None, resolved: str = None,
            date_from: str | datetime = None, date_to: str | datetime = None,
            before: str | datetime = None, after: str | datetime = None,
            min_age: timedelta = None, max_age: timedelta = None,
            with_source_assets: bool = None, with_source_devices: bool = None,
            **kwargs):
        """Query the database and delete matching alarms.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification. Filters can be
        combined (as defined in the Cumulocity REST API).

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            type (str):  Alarm type
            source (str):  Database ID of a source device
            fragment (str):  Name of a present custom/standard fragment
            status (str):  Alarm status
            severity (str):  Alarm severity
            resolved (str):  Whether the alarm status is CLEARED
            before (str|datetime):  Datetime object or ISO date/time string.
                Only alarms assigned to a time before this date are returned.
            after (str|datetime):  Datetime object or ISO date/time string.
                Only alarms assigned to a time after this date are returned
            date_from (str|datetime): Same as `after`
            date_to (str|datetime): Same as `before`
            min_age (timedelta):  Matches only alarms of at least this age
            max_age (timedelta):  Matches only alarms with at most this age
            with_source_assets (bool): Whether also alarms for related source
                assets should be included. Requires `source`.
            with_source_devices (bool): Whether also alarms for related source
                devices should be included. Requires `source`
        """
        # build a base query
        base_query = self._prepare_query(
            expression=expression,
            type=type, source=source, fragment=fragment,
            status=status, severity=severity, resolved=resolved,
            before=before, after=after, min_age=min_age, max_age=max_age,
            date_from=date_from, date_to=date_to,
            with_source_devices=with_source_devices, with_source_assets=with_source_assets,
            **kwargs)
        self.c8y.delete(base_query)
