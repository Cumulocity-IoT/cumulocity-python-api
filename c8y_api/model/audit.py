# Copyright (c) 2021 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Generator, List

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model._base import CumulocityResource, ComplexObject
from c8y_api.model._parser import ComplexObjectParser
from c8y_api.model._util import _DateUtil


class AuditRecord(ComplexObject):
    """Represents an Audit Record object within Cumulocity.

    Instances of this class are returned by functions of the corresponding
    Audits API. Use this class to create new or update AuditRecord objects.

    See also:  https://cumulocity.com/api/core/#tag/Audits
    """

    class Severity:
        """Audit severity levels."""
        MAJOR = 'MAJOR'
        CRITICAL = 'CRITICAL'
        MINOR = 'MINOR'
        WARNING = 'WARNING'
        INFORMATION = 'information'  # for whatever reason, this is used.

    _parser = ComplexObjectParser({
            'type': 'type',
            'time': 'time',
            'creation_time': 'creationTime',
            'activity': 'activity',
            'text': 'text',
            'severity': 'severity',
            'user': 'user',
            'application': 'application'}, [] )
    _resource = '/audit/auditRecords'
    _accept = CumulocityRestApi.CONTENT_AUDIT_RECORD

    def __init__(self, c8y: CumulocityRestApi = None, type: str = None, time: str | datetime = None,  # noqa (type)
                 source: str = None, activity: str = None, text: str = None, severity: str = None,
                 application: str = None, user: str = None, **kwargs):
        """Create a new AuditRecord object.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            type (str):  Audit records type
            time (str|datetime):  Date/time of the audit records Can be
                provided as timezone-aware datetime object or formatted
                string (in standard ISO format incl. timezone:
                YYYY-MM-DD'T'HH:MM:SS.SSSZ as it is returned by the
                Cumulocity REST API).
                Use 'now' to set  to current datetime in UTC.
            source (str):  The managed object ID to which the audit is associated
            activity (str):  Summary of the action that was carried out
            text (str):  Details of the action that was carried out
            severity (str):  Severity of the audit record.
            application (str):  The application from which the record was created.
            user (str):  The user who carried out the activity
            kwargs:  Additional arguments are treated as custom fragments
        """
        super().__init__(c8y, **kwargs)
        self.type = type
        self.time = _DateUtil.ensure_timestring(time)
        self.creation_time = None  # undocumented property
        self.source = source
        self.activity = activity
        self.text = text
        self.severity = severity   # undocumented property
        self.application = application
        self.user = user

    @property
    def datetime(self) -> datetime:
        """Convert the audit record's time to a Python datetime object.

        Returns:
            Standard Python datetime object
        """
        return super()._to_datetime(self.time)

    @property
    def creation_datetime(self) -> datetime:
        """Convert the audit record's creation time to a Python
        datetime object.

        Returns:
            Standard Python datetime object
        """
        return super()._to_datetime(self.creation_time)

    @classmethod
    def from_json(cls, json: dict) -> AuditRecord:
        # (no doc update required)
        obj = super()._from_json(json, AuditRecord())
        obj.source = json['source']['id']
        return obj

    def to_json(self, only_updated: bool = False) -> dict:
        # (no doc update required)
        obj_json = super()._to_json(only_updated, exclude={'creation_time'})
        # source needs to be set manually, but it cannot be updated
        if not only_updated and self.source:
            obj_json['source'] = {'id': self.source}
        return obj_json

    def create(self) -> AuditRecord:
        """Create the AuditRecord within the database.

        Returns:
            A fresh AuditRecord object representing what was
            created within the database (including the ID).
        """
        return super()._create()


class AuditRecords(CumulocityResource):
    """Provides access to the Audit API.

    This class can be used for get, search for, create, update and
    delete records within the Cumulocity database.

    See also:  https://cumulocity.com/api/core/#tag/Audits
    """

    def __init__(self, c8y):
        super().__init__(c8y, '/audit/auditRecords')

    def get(self, record_id: str) -> AuditRecord:
        """Retrieve a specific object from the database.

        Args:
            record_id (str):  The database ID of the audit record

        Returns:
            An AuditRecord instance representing the object in the database.
        """
        audit_obj = AuditRecord.from_json(self._get_object(record_id))
        audit_obj.c8y = self.c8y  # inject c8y connection into instance
        return audit_obj

    def select(self, type: str = None, source: str = None, application: str = None, user: str = None, # noqa (type)
               before: str | datetime = None, after: str | datetime = None,
               min_age: timedelta = None, max_age: timedelta = None,
               reverse: bool = False, limit: int = None, page_size: int = 1000) -> Generator[AuditRecord]:
        """Query the database for audit records and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters' specification.  Filters can be
        combined (within reason).

        Args:
            type (str):  Audit record type
            source (str):  Database ID of a source device
            application (str):  Application from which the audit was carried out.
            user (str): The user who carried out the activity.
            before (str|datetime):  Datetime object or ISO date/time string. Only
                records assigned to a time before this date are returned.
            after (str|datetime):  Datetime object or ISO date/time string. Only
                records assigned to a time after this date are returned.
            min_age (timedelta): Minimum age for selected records.
            max_age (timedelta): Maximum age for selected records.
            reverse (bool): Invert the order of results, starting with the
                most recent one.
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of objects which are read (and
                parsed in one chunk). This is a performance related setting.

        Returns:
            Generator for AuditRecord objects
        """
        base_query = self._build_base_query(type=type, source=source, application=application, user=user,
                                            before=before, after=after,
                                            min_age=min_age, max_age=max_age,
                                            reverse=reverse, page_size=page_size)
        return super()._iterate(base_query, limit, AuditRecord.from_json)

    def get_all(self, type: str = None, source: str = None, application: str = None, user: str = None,  # noqa (type)
               before: str | datetime = None, after: str | datetime = None,
               min_age: timedelta = None, max_age: timedelta = None,
               reverse: bool = False, limit: int = None, page_size: int = 1000) -> List[AuditRecord]:
        """Query the database for audit records and return the results as list.

        This function is a greedy version of the `select` function. All
        available results are read immediately and returned as list.

        See `select` for a documentation of arguments.

        Returns:
            List of AuditRecord objects
        """
        return list(self.select(type=type, source=source, application=application, user=user,
                                before=before, after=after,
                                min_age=min_age, max_age=max_age,
                                reverse=reverse, limit=limit, page_size=page_size))

    def create(self, *records: AuditRecord):
        """Create audit record objects within the database.

        Note: If not yet defined, this will set the record date to now in
            each of the given objects.

        Args:
            records (*AuditRecord):  Collection of AuditRecord instances
        """
        for r in records:
            if not r.time:
                r.time = _DateUtil.to_timestring(datetime.utcnow())
        super()._create(AuditRecord.to_full_json, *records)