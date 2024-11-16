# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

# Alarm and Event are similar by design, hence
# pylint: disable=duplicate-code

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Generator, List, BinaryIO

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model._base import CumulocityResource, SimpleObject, ComplexObject
from c8y_api.model._parser import ComplexObjectParser
from c8y_api.model._util import _DateUtil


class Event(ComplexObject):
    """Represent an instance of an event object in Cumulocity.

    Instances of this class are returned by functions of the corresponding
    Events API. Use this class to create new or update Event objects.

    See also: https://cumulocity.com/api/#tag/Events
    """

    _resource = '/event/events'
    _accept = 'application/vnd.com.nsn.cumulocity.event+json'
    _parser = ComplexObjectParser({
        'type': 'type',
        'time': 'time',
        '_u_text': 'text',
        'creation_time': 'creationTime',
        'updated_time': 'lastUpdated',
        }, ['source'])

    def __init__(self, c8y: CumulocityRestApi = None, type: str = None, time: str | datetime = None,  # noqa (type)
                 source: str = None, text: str = None, **kwargs):
        """Create a new Event object.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            type (str):  Event type
            time (str|datetime):  Date/time of the event. Can be provided as
                timezone-aware datetime object or formatted string (in
                standard ISO format incl. timezone: YYYY-MM-DD'T'HH:MM:SS.SSSZ
                as it is returned by the Cumulocity REST API).
                Use 'now' to set  to current datetime in UTC.
            source (str):  ID of the device which this event is raised by
            text (str):  Event test/description
            kwargs:  Additional arguments are treated as custom fragments
        """
        super().__init__(c8y=c8y, **kwargs)
        self.type = type
        self.time = _DateUtil.ensure_timestring(time)
        self.source = source
        self._u_text = text
        self.creation_time = None
        self.updated_time = None

    text = SimpleObject.UpdatableProperty('_u_text')

    @property
    def datetime(self) -> datetime:
        """Convert the event's time to a Python datetime object.

        Returns:
            Standard Python datetime object
        """
        return super()._to_datetime(self.time)

    @property
    def creation_datetime(self) -> datetime:
        """Convert the event's creation time to a Python datetime object.

        Returns:
            Standard Python datetime object
        """
        return super()._to_datetime(self.creation_time)

    @property
    def updated_datetime(self) -> datetime:
        """Convert the alarm's last updated time to a Python datetime object.

        Returns:
            Standard Python datetime object for the alarm's last updated time.
        """
        return super()._to_datetime(self.updated_time)

    def _build_attachment_path(self) -> str:
        return super()._build_object_path() + '/binaries'

    @classmethod
    def from_json(cls, json: dict) -> Event:
        # (no doc update required)
        obj = super()._from_json(json, Event())
        obj.source = json['source']['id']
        return obj

    def to_json(self, only_updated: bool = False) -> dict:
        # (no doc update required)
        # creation time is always excluded
        obj_json = super()._to_json(only_updated, exclude={'creation_time'})
        # source needs to be set manually, but it cannot be updated
        if not only_updated and self.source:
            obj_json['source'] = {'id': self.source}
        return obj_json

    def create(self) -> Event:
        """Create the Event within the database.

        Returns:
            A fresh Event object representing what was
            created within the database (including the ID).
        """
        return super()._create()

    def update(self) -> Event:
        """Update the Event within the database.

        Note: This will only send changed fields to increase performance.

        Returns:
            A fresh Event object representing what the updated
            state within the database (including the ID).
        """
        return super()._update()

    def delete(self):
        """Delete the Event within the database."""
        super()._delete()

    def apply_to(self, other_id: str) -> Event:
        """Apply changes made to this object to another object in the
            database.

        Args:
            other_id (str):  Database ID of the event to update.

        Returns:
            A fresh Event instance representing the updated object
            within the database.

        See also function `Events.apply_to` which doesn't parse the result.
        """
        return super()._apply_to(other_id)

    def has_attachment(self) -> bool:
        """Check whether the event has a binary attachment.

        Event objects that have an attachment feature a `c8y_IsBinary`
        fragment. This function checks the presence of that fragment.

        Note: This does not query the database. Hence, the information might
        be outdated if a binary was attached _after_ the event object was
        last read from the database.

        Returns:
            True if the event object has an attachment, False otherwise.
        """
        return 'c8y_IsBinary' in self

    def download_attachment(self) -> bytes:
        """Read the binary attachment.

        Returns:
            The event's binary attachment as bytes.
        """
        super()._assert_c8y()
        super()._assert_id()
        return self.c8y.get_file(self._build_attachment_path())

    def create_attachment(self, file: str | BinaryIO, content_type: str = None) -> dict:
        """Create the binary attachment.

        Args:
            file (str|BinaryIO): File-like object or a file path
            content_type (str):  Content type of the file sent
                (default is application/octet-stream)

        Returns:
            Attachment details as JSON object (dict).
        """
        super()._assert_c8y()
        super()._assert_id()
        return self.c8y.post_file(self._build_attachment_path(), file,
                                  accept='application/json', content_type=content_type)

    def update_attachment(self, file: str | BinaryIO, content_type: str = None) -> dict:
        """Update the binary attachment.

        Args:
            file (str|BinaryIO): File-like object or a file path
            content_type (str):  Content type of the file sent
                (default is application/octet-stream)

        Returns:
            Attachment details as JSON object (dict).
        """
        super()._assert_c8y()
        super()._assert_id()
        return self.c8y.put_file(self._build_attachment_path(), file,
                                 accept='application/json', content_type=content_type)

    def delete_attachment(self):
        """Remove the binary attachment."""
        super()._assert_c8y()
        super()._assert_id()
        self.c8y.delete(self._build_attachment_path())


class Events(CumulocityResource):
    """Provides access to the Events API.

    This class can be used for get, search for, create, update and
    delete events within the Cumulocity database.

    See also: https://cumulocity.com/api/#tag/Events
    """

    def __init__(self, c8y):
        super().__init__(c8y, '/event/events')

    def build_attachment_path(self, event_id: str) -> str:
        """Build the attachment path of a specific event.

        Args:
            event_id (int|str):  Database ID of the event

        Returns:
            The relative path to the event attachment within Cumulocity.
        """
        return super().build_object_path(event_id) + '/binaries'

    def get(self, event_id: str) -> Event:  # noqa (id)
        """Retrieve a specific object from the database.

        Args:
            event_id (str):  The database ID of the event

        Returns:
            An Event instance representing the object in the database.
        """
        event_object = Event.from_json(self._get_object(event_id))
        event_object.c8y = self.c8y  # inject c8y connection into instance
        return event_object

    @staticmethod
    def _check_params(fragment, fragment_type, fragment_value):
        """Check for invalid select parameter combinations."""
        if fragment_value and not (fragment_type or fragment):
            raise ValueError("Fragment value filter also needs 'fragment_type' or 'fragment' filter.")

    def select(self,
               expression: str = None,
               type: str = None, source: str = None, fragment: str = None,  # noqa (type)
               fragment_type: str = None, fragment_value: str = None,
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
               **kwargs) -> Generator[Event]:
        """Query the database for events and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filter's specification.  Filters can be
        combined (within reason).

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            type (str):  Event type
            source (str):  Database ID of a source device
            fragment (str):  Name of a present custom/standard fragment
            fragment_type (str):  Name of a present custom/standard fragment.
            fragment_value (str):  Value of present custom/standard fragment.
            before (str|datetime):  Datetime object or ISO date/time string. Only
                events assigned to a time before this date are returned.
            after (str|datetime):  Datetime object or ISO date/time string. Only
                events assigned to a time after this date are returned.
            created_before (str|datetime):  Datetime object or ISO date/time string.
                Only events changed at a time before this date are returned.
            created_after (str|datetime):  Datetime object or ISO date/time string.
                Only events changed at a time after this date are returned.
            updated_before (str|datetime):  Datetime object or ISO date/time string.
                Only events changed at a time before this date are returned.
            updated_after (str|datetime):  Datetime object or ISO date/time string.
                Only events changed at a time after this date are returned.
            min_age (timedelta): Minimum age for selected events.
            max_age (timedelta): Maximum age for selected events.
            date_from (str|datetime): Same as `after`
            date_to (str|datetime): Same as `before`
            created_from (str|datetime): Same as `created_after`
            created_to(str|datetime): Same as `created_before`
            last_updated_from (str|datetime): Same as `updated_after`
            last_updated_to (str|datetime): Same as `updated_before`
            reverse (bool): Invert the order of results, starting with the
                most recent one.
            with_source_assets (bool): Whether also alarms for related source
                assets should be included. Requires `source`.
            with_source_devices (bool): Whether also alarms for related source
                devices should be included. Requires `source`
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of events which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator for Event objects
        """
        Events._check_params(fragment, fragment_type, fragment_value)

        base_query = self._prepare_query(
            expression=expression,
            type=type, source=source, fragment=fragment,
            fragment_type=fragment_type, fragment_value=fragment_value,
            before=before, after=after,
            date_from=date_from, date_to=date_to,
            created_before=created_before, created_after=created_after,
            created_from=created_from, created_to=created_to,
            updated_before=updated_before, updated_after=updated_after,
            last_updated_from=last_updated_from, last_updated_to=last_updated_to,
            min_age=min_age, max_age=max_age,
            reverse=reverse,
            with_source_devices=with_source_devices, with_source_assets=with_source_assets,
            page_size=page_size,
            **kwargs)
        return super()._iterate(base_query, page_number, limit, Event.from_json)

    def get_all(
            self,
            expression: str = None,
            type: str = None, source: str = None, fragment: str = None,  # noqa (type)
            fragment_type: str = None, fragment_value: str = None,
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
            **kwargs) -> List[Event]:
        """Query the database for events and return the results as list.

        This function is a greedy version of the `select` function. All
        available results are read immediately and returned as list.

        See `select` for a documentation of arguments.

        Returns:
            List of Event objects
        """
        return list(self.select(
            expression=expression,
            type=type, source=source, fragment=fragment,
            fragment_type=fragment_type, fragment_value=fragment_value,
            before=before, after=after,
            date_from=date_from, date_to=date_to,
            created_before=created_before, created_after=created_after,
            created_from=created_from, created_to=created_to,
            updated_before=updated_before, updated_after=updated_after,
            last_updated_from=last_updated_from, last_updated_to=last_updated_to,
            min_age=min_age, max_age=max_age,
            reverse=reverse,
            with_source_devices=with_source_devices, with_source_assets=with_source_assets,
            limit=limit, page_size=page_size, page_number=page_number,
            **kwargs
        ))

    def get_count(
            self,
            expression: str = None,
            type: str = None, source: str = None, fragment: str = None,  # noqa (type)
            fragment_type: str = None, fragment_value: str = None,
            before: str | datetime = None, after: str | datetime = None,
            date_from: str | datetime = None, date_to: str | datetime = None,
            created_before: str | datetime = None, created_after: str | datetime = None,
            created_from: str | datetime = None, created_to: str | datetime = None,
            updated_before: str | datetime = None, updated_after: str | datetime = None,
            last_updated_from: str | datetime = None, last_updated_to: str | datetime = None,
            min_age: timedelta = None, max_age: timedelta = None,
            reverse: bool = False, limit: int = None,
            with_source_assets: bool = None, with_source_devices: bool = None,
            **kwargs
    ) -> int:
        """Calculate the number of potential results of a database query.

        This function uses the same parameters as the `select` function.

        Returns:
            Number of potential results
        """
        Events._check_params(
            fragment=fragment,
            fragment_type=fragment_type,
            fragment_value=fragment_value,
        )
        base_query = self._prepare_query(
            expression=expression,
            type=type, source=source, fragment=fragment,
            fragment_type=fragment_type, fragment_value=fragment_value,
            before=before, after=after,
            date_from=date_from, date_to=date_to,
            created_before=created_before, created_after=created_after,
            created_from=created_from, created_to=created_to,
            updated_before=updated_before, updated_after=updated_after,
            last_updated_from=last_updated_from, last_updated_to=last_updated_to,
            min_age=min_age, max_age=max_age,
            reverse=reverse,
            with_source_devices=with_source_devices, with_source_assets=with_source_assets,
            limit=limit, page_size=1,
            **kwargs
        )
        return self._get_count(base_query)

    def create(self, *events: Event):
        """Create event objects within the database.

        Note: If not yet defined, this will set the event date to now in
            each of the given event objects.

        Args:
            *events (Event):  Collection of Event instances
        """
        for e in events:
            if not e.time:
                e.time = _DateUtil.to_timestring(datetime.utcnow())
        super()._create(Event.to_full_json, *events)

    def update(self, *events: Event):
        """Write changes to the database.

        Args:
            *events (Event):  Collection of Event instances
        """
        super()._update(Event.to_diff_json, *events)

    def apply_to(self, event: Event | dict, *event_ids: str):
        """Apply changes made to a single instance to other objects in the
        database.

        Args:
            event (Event|dict): Event used as model for the update or simply
                a dictionary representing the diff JSON.
            *event_ids (str):  Collection of ID of the events to update
        """
        super()._apply_to(Event.to_full_json, event, *event_ids)

    # delete function is defined in super class

    def delete_by(
            self,
            expression: str = None,
            type: str = None, source: str = None, fragment: str = None,
            before: str | datetime = None, after: str | datetime = None,
            min_age: timedelta = None, max_age: timedelta = None,
            **kwargs):
        """Query the database and delete matching events.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filter's specification.  Filters can be
        combined (within reason).

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            type (str):  Event type
            source (str):  Database ID of a source device
            fragment (str):  Name of a present custom/standard fragment
            before (str|datetime):  Datetime object or ISO date/time string. Only
                events assigned to a time before this date are returned.
            after (str|datetime):  Datetime object or ISO date/time string. Only
                events assigned to a time after this date are returned.
            min_age (timedelta): Minimum age for selected events.
            max_age (timedelta): Maximum age for selected events.
        """
        # prepare for future support
        Events._check_params(fragment, kwargs.get('fragment_type', None), kwargs.get('fragment_value'))
        # build a base query
        base_query = self._prepare_query(
            expression=expression,
            type=type, source=source, fragment=fragment,
            before=before, after=after, min_age=min_age, max_age=max_age,
            **kwargs)
        self.c8y.delete(base_query)

    def create_attachment(self, event_id: str, file: str | BinaryIO, content_type: str = None) -> dict:
        """Add an event's binary attachment.

        Args:
            event_id (str):  The database ID of the event
            file (str|BinaryIO): File-like object or a file path
            content_type (str):  Content type of the file sent
                (default is application/octet-stream)

        Returns:
            Attachment details as JSON object (dict).
        """
        return self.c8y.post_file(self.build_attachment_path(event_id), file,
                                  accept='application/json', content_type=content_type)

    def update_attachment(self, event_id: str, file: str | BinaryIO, content_type: str = None) -> dict:
        """Update an event's binary attachment.

        Args:
            event_id (str):  The database ID of the event
            file (str|BinaryIO): File-like object or a file path
            content_type (str):  Content type of the file sent
                (default is application/octet-stream)

        Returns:
            Attachment details as JSON object (dict).
        """
        return self.c8y.put_file(self.build_attachment_path(event_id), file,
                                 accept='application/json', content_type=content_type)

    def download_attachment(self, event_id: str) -> bytes:
        """Read an event's binary attachment.

        Args:
            event_id (str):  The database ID of the event

        Returns:
            The event's binary attachment as bytes.
        """
        return self.c8y.get_file(self.build_attachment_path(event_id))

    def delete_attachment(self, event_id: str):
        """Remove an event's binary attachment.

        Args:
            event_id (str):  The database ID of the event
        """
        self.c8y.delete(self.build_attachment_path(event_id))
