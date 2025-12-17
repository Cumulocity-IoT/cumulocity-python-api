# Copyright (c) 2025 Cumulocity GmbH

import re
from datetime import datetime, timedelta, timezone
from typing import Union

from dateutil import parser
from pandas import to_datetime


class _StringUtil(object):

    TO_PASCAL_PATTERN = re.compile(r'_([a-z])')

    @staticmethod
    def concat(*strings:Union[str, None]):
        """Concatenate non-None strings."""
        return ''.join(x for x in strings if x)

    @staticmethod
    def concat_with(sep: str, *strings:Union[str, None]):
        """Concatenate non-None strings with separator."""
        return sep.join(x for x in strings if x)

    @staticmethod
    def to_pascal_case(name: str):
        """Convert a given snake case (default Python style) name to pascal case (default for names in Cumulocity)"""
        parts = list(filter(None, name.split('_')))
        if len(parts) == 1:
            return name
        return parts[0] + "".join([x.title() for x in parts[1:]])

    @staticmethod
    def like(expression: str, string: str):
        """Check if like-expression matches a string.

        Only supports * at beginning and end.
        """
        return (
            expression[1:-1] in string if expression.startswith('*') and expression.endswith('*')
            else string.startswith(expression[:-1]) if expression.endswith('*')
            else string.endswith(expression[1:]) if expression.startswith('*')
            else expression == string
        )

    @staticmethod
    def matches(expression: str, string: str):
        """Check if regex expression matches a string."""
        try:
            return re.search(expression, string) is not None
        except re.error:
            return False

class _QueryUtil(object):

    @staticmethod
    def encode_odata_query_value(value):
        """Encode value strings according to OData query rules.
        http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part2-url-conventions.html#sec_URLParsing
        http://docs.oasis-open.org/odata/odata/v4.01/cs01/abnf/odata-abnf-construction-rules.txt """
        # single quotes escaped through single quote
        return re.sub('\'', '\'\'', value)

    @staticmethod
    def encode_odata_text_value(value):
        """Encode value strings according to OData query rules.
        http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part2-url-conventions.html#sec_URLParsing
        http://docs.oasis-open.org/odata/odata/v4.01/cs01/abnf/odata-abnf-construction-rules.txt """
        # single quotes escaped through single quote
        encoded_quotes = re.sub('\'', '\'\'', value)
        return encoded_quotes if " " not in encoded_quotes else f"'{encoded_quotes}'"


class _DateUtil(object):

    @staticmethod
    def now_timestring() -> str:
        """Provide an ISO timestring for the current time."""
        return _DateUtil.to_timestring(_DateUtil.now())

    @staticmethod
    def to_timestring(dt: datetime):
        """Format a datetime as ISO timestring."""
        return dt.isoformat(timespec='milliseconds')

    @staticmethod
    def to_datetime(string):
        """Parse an ISO timestring as datetime object."""
        return parser.parse(string)

    @staticmethod
    def now():
        """Provide the current time as datetime object."""
        return datetime.now(timezone.utc)

    @staticmethod
    def ensure_datetime(arg):
        """Ensure a datetime object."""
        if isinstance(arg, datetime):
            return arg
        return to_datetime(arg)

    @staticmethod
    def ensure_timestring(time):
        """Ensure that a given timestring reflects a proper, timezone aware date/time.
        A static string 'now' will be converted to the current datetime in UTC."""
        if isinstance(time, datetime):
            if not time.tzinfo:
                raise ValueError("A specified datetime needs to be timezone aware.")
            return _DateUtil.to_timestring(time)
        if time == 'now':
            return _DateUtil.now_timestring()
        return time  # assuming it is a timestring

    @staticmethod
    def ensure_timedelta(time):
        """Ensure that a given object is a timedelta object."""
        if not isinstance(time, timedelta):
            raise ValueError("A specified duration needs to be a timedelta object.")
        return time
