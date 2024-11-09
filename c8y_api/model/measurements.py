# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta
from typing import Type, List, Generator, Sequence
from urllib.parse import urlencode

from c8y_api._base_api import CumulocityRestApi

from c8y_api.model._base import CumulocityResource, ComplexObject
from c8y_api.model._parser import ComplexObjectParser
from c8y_api.model._base import _DictWrapper
from c8y_api.model._util import _DateUtil


class Units(object):
    """Predefined, common units."""
    Grams = 'g'
    Kilograms = 'kg'
    Kelvin = 'K'
    Celsius = '°C'
    Fahrenheit = '°F'
    Meters = 'm'
    Centimeters = 'cm'
    Millimeters = 'mm'
    Liters = 'l'
    CubicMeters = 'm3'
    Count = '#'
    Percent = '%'


class Value(dict):
    """Generic datapoint."""
    def __init__(self, value, unit):
        super().__init__(value=value, unit=unit)


class Grams(Value):
    """Weight datapoint (Grams)."""
    def __init__(self, value):
        super().__init__(value, Units.Grams)


class Kilograms(Value):
    """Weight datapoint (Kilograms)."""
    def __init__(self, value):
        super().__init__(value, Units.Kilograms)


class Kelvin(Value):
    """Temperature datapoint (Kelvin)."""
    def __init__(self, value):
        super().__init__(value, Units.Kelvin)


class Celsius(Value):
    """Temperature datapoint (Celsius)."""
    def __init__(self, value):
        super().__init__(value, Units.Celsius)


class Meters(Value):
    """Length datapoint (Meters)."""
    def __init__(self, value):
        super().__init__(value, Units.Meters)


class Centimeters(Value):
    """Length datapoint (Centimeters)."""
    def __init__(self, value):
        super().__init__(value, Units.Centimeters)


class Millimeters(Value):
    """Length datapoint (Millimeters)."""
    def __init__(self, value):
        super().__init__(value, Units.Millimeters)


class Liters(Value):
    """Volume datapoint (Liters)."""
    def __init__(self, value):
        super().__init__(value, Units.Liters)


class CubicMeters(Value):
    """Volume datapoint (Cubic Meters)."""
    def __init__(self, value):
        super().__init__(value, Units.CubicMeters)


class Count(Value):
    """Discrete number datapoint (number/count)."""
    def __init__(self, value):
        super().__init__(value, Units.Count)

class Percentage(Value):
    """Percent value datapoint."""
    def __init__(self, value):
        super().__init__(value, Units.Percent)


class Measurement(ComplexObject):
    """ Represents an instance of a measurement object in Cumulocity.

    Instances of this class are returned by functions of the corresponding
    Measurements API. Use this class to create new or update existing
    measurements.

    See also: https://cumulocity.com/guides/reference/measurements/#measurement
    """

    # these need to be defined like this for the abstract super functions
    _resource = '/measurement/measurements'
    _parser = ComplexObjectParser({'type': 'type', 'time': 'time'}, ['source'])
    # _accept
    # _not_updatable

    def __init__(self, c8y=None, type=None, source=None, time: str|datetime = None, **kwargs):  # noqa (type)
        """ Create a new Measurement object.

        Args:
            c8y (CumulocityRestApi)  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            type (str)  Measurement type
            source (str)  Device ID which this measurement is for
            time(str|datetime):  Datetime string or Python datetime object. A
                given datetime string needs to be in standard ISO format incl.
                timezone: YYYY-MM-DD'T'HH:MM:SS.SSSZ as it is returned by the
                Cumulocity REST API. A given datetime object needs to be
                timezone aware. For manual construction it is recommended to
                specify a datetime object as the formatting of a timestring
                is never checked for performance reasons.
            kwargs:  All additional named arguments are interpreted as
                custom fragments e.g. for data points.

        Returns:
            Measurement object
        """
        super().__init__(c8y, **kwargs)
        self.type = type
        self.source = source
        # The time can either be set as string (e.g. when read from JSON) or
        # as a datetime object. It will be converted to string immediately
        # as there is no scenario where a manually created object won't be
        # written to Cumulocity anyway
        self.time = _DateUtil.ensure_timestring(time)

    @classmethod
    def from_json(cls, json) -> Measurement:
        """ Build a new Measurement instance from Cumulocity JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Args:
            json (dict)  JSON object (nested dictionary)
                representing a measurement within Cumulocity

        Returns:
            Measurement object
        """
        obj = cls._from_json(json, Measurement())
        obj.source = json['source']['id']
        return obj

    def to_json(self, only_updated=False) -> dict:
        """ Convert the instance to JSON.

        The JSON format produced by this function is what is used by the
        Cumulocity REST API.

        Note: Measurements cannot be updated, hence this function does not
        feature an only_updated argument.

        Returns:
            JSON object (nested dictionary)
        """
        if only_updated:
            raise NotImplementedError('The Measurement class does not support incremental updates.')
        measurement_json = super().to_json()
        measurement_json['source'] = {'id': self.source}
        if not self.time:
            measurement_json['time'] = _DateUtil.to_timestring(_DateUtil.now())
        return measurement_json

    # the __getitem__ function is overwritten to return a wrapper that doesn't signal updates
    # (because Measurements are not updated, can only be created from scratch)
    def __getitem__(self, item):
        return _DictWrapper(self.fragments[item], on_update=None)

    def get_series(self) -> list[str]:
        """Collect series names.

        Collect series names defined in this measurement. Any top level fragment having a nested element
        featuring a _value_ field is considered a series. Multiple such series could be defined.

        ```json
        {
            "c8y_Temperature": {
                "T": {
                    "unit": "C",
                    "value": 12.8
                }
            }
        }
        ```

        Returns:
            A list of series names (e.g. `c8y_Temperature.T`) defined in this measurement.
        """
        return [f'{k1}.{k2}' for k1, v1 in self.fragments.items() for k2, v2 in v1.items() if 'value' in v2]

    @property
    def datetime(self) -> Type[datetime] | None:
        """ Convert the measurement's time to a Python datetime object.

        Returns:
            (datetime): The measurement's time
        """
        if self.time:
            return _DateUtil.to_datetime(self.time)
        return None

    def create(self) -> Measurement:
        """ Store the Measurement within the database.

        Returns:  A fresh Measurement object representing what was
            created within the database (including the ID).
        """
        return self._create()

    def update(self) -> Measurement:
        """Not implemented for Measurements."""
        raise NotImplementedError('Measurements cannot be updated within Cumulocity.')

    def delete(self):
        """Delete the Measurement within the database."""
        self._delete()


class Series(dict):
    """ A wrapper for a series result.

    See also: `Measurements.get_series` function

    This class wraps the raw JSON result but can also be used to read result specs
    and collect result values conveniently.

    See also: https://cumulocity.com/api/core/#operation/getMeasurementSeriesResource
    """

    @dataclasses.dataclass
    class SeriesSpec:
        """Series specifications."""
        unit: str
        type: str
        name: str

        @property
        def series(self):
            """Return the complete series name."""
            return f'{self.type}.{self.name}'

    @property
    def truncated(self):
        """Whether the result was truncated
        (i.e. the query returned more than 5000 values)."""
        return self['truncated']

    @property
    def specs(self) -> Sequence[SeriesSpec]:
        """Return specifications for all enclosed series."""
        return [self.SeriesSpec(type=i['type'], name=i['name'], unit=i['unit']) for i in self['series']]

    def collect(self, series: str | Sequence[str] = None, value: str = None,
                timestamps: bool | str = None) -> List | List[tuple]:
        """Collect series results.

        Args:
            series (str|Sequence[str]):  Which series' values to collect. If
                multiple series are collected each element in the result will
                be a tuple. If omitted, all available series are collected.
            value (str):  Which value (min/max) to collect. If omitted, both
                values will be collected, grouped as 2-tuples.
            timestamps (bool|str):  Whether each element in the result list will
                be prepended with the corresponding timestamp. If True, the
                timestamp string will be included; Use 'datetime' or 'epoch' to
                parse the timestamp string.

        Returns:
            A simple list or list of tuples (potentially nested) depending on the
            parameter combination.
        """
        # we want explicit else's to make the logic easier to understand
        # pylint: disable=no-else-return, too-many-return-statements, too-many-branches, line-too-long

        def indexes_by_name():
            """Mapping series names to indexes in value groups."""
            return {f'{s[1].type}.{s[1].name}': s[0] for s in enumerate(self.specs)}

        def parse_timestamp(t):
            """Parse timestamps."""
            if timestamps == 'datetime':
                return datetime.fromisoformat(t)
            if timestamps == 'epoch':
                return datetime.fromisoformat(t).timestamp()
            return t

        # use all series if no series provided
        if not series:
            series = [s.series for s in self.specs]

        # single series
        if isinstance(series, str):
            # which index to pull from values?
            i = indexes_by_name()[series]

            # single value
            if value:
                if not timestamps:
                    # iterate over all values, select value group at specific
                    # index v[i] and extract specific value [value]. The value
                    # group may be undefined (None), hence filter for value v[i]
                    return [v[i][value] for v in self['values'].values() if (len(v) > i and v[i])]
                else:
                    # like above, but include timestamps
                    return [(parse_timestamp(k), v[i][value]) for k, v in self['values'].items() if (len(v) > i and v[i])]

            # all values
            else:
                if not timestamps:
                    # iterate over all values, select value group at specific
                    # index v[i] and extract both values (min, max). The value
                    # group may be undefined (None), hence filter for value v[i]
                    return [(v[i]['min'], v[i]['max']) for v in self['values'].values() if (len(v) > i and v[i])]
                else:
                    # like above, but include timestamps
                    return [(parse_timestamp(k), v[i]['min'], v[i]['max']) for k, v in self['values'].items() if (len(v) > i and v[i])]

        # multiple series
        if isinstance(series, Sequence):
            ii = [indexes_by_name()[s] for s in series]

            # single value
            if value:
                if not timestamps:
                    # iterate over all values, collect specified value groups
                    # at their index v[i] and extract specific value [value].
                    # The value group may be undefined (None) which will result
                    # in a None value in the tuple as well.
                    return [
                        # collect values of all indexes (None of not defined)
                        tuple(v[i][value] if (len(v) > i and v[i]) else None for i in ii)
                        for v in self['values'].values()
                    ]
                else:
                    # like above, but prepend with timestamps
                    return [
                        (parse_timestamp(k), *(v[i][value] if (len(v) > i and v[i]) else None for i in ii))
                        for k, v in self['values'].items()
                    ]

            # all values
            else:
                if not timestamps:
                    # iterate over all values, collect specified value groups
                    # at their index v[i] and extract specific value [value].
                    # The value group may be undefined (None) which will result
                    # in a None value in the tuple as well.
                    return [
                        # collect values of all indexes (None of not defined)
                        tuple((v[i]['min'], v[i]['max']) if (len(v) > i and v[i]) else None for i in ii)
                        for v in self['values'].values()
                    ]
                else:
                    # like above, but prepend with timestamps
                    return [
                        (parse_timestamp(k), *((v[i]['min'], v[i]['max']) if (len(v) > i and v[i]) else None for i in ii))
                        for k, v in self['values'].items()
                    ]

        raise ValueError("Invalid combination of arguments")


class Measurements(CumulocityResource):
    """ A wrapper for the standard Measurements API.

    This class can be used for get, search for, create, update and
    delete measurements within the Cumulocity database.

    See also: https://cumulocity.com/guides/reference/measurements/#measurement
    """

    class AggregationType:
        """Series aggregation types."""
        DAILY = 'DAILY'
        HOURLY = 'HOURLY'
        MINUTELY = 'MINUTELY'

    def __init__(self, c8y: CumulocityRestApi):
        super().__init__(c8y, 'measurement/measurements')

    def get(self, measurement_id: str | int) -> Measurement:
        """ Read a specific measurement from the database.

        Args:
            measurement_id (str|int):  database ID of a measurement

        Returns:
            Measurement object

        Raises:
            KeyError:  if the ID cannot be resolved.
        """
        measurement = Measurement.from_json(self._get_object(measurement_id))
        measurement.c8y = self.c8y  # inject c8y connection into instance
        return measurement

    def select(
            self,
            type: str = None,
            source: str | int = None,
            fragment: str = None,
            value: str = None,
            series: str = None,
            before: str | datetime = None,
            after: str | datetime = None,
            min_age: timedelta = None,
            max_age: timedelta = None,
            reverse: bool = None,
            limit: int = None,
            page_size: int = 1000,
            page_number: int = None
    ) -> Generator[Measurement]:
        """ Query the database for measurements and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification.  Filters can be
        combined (within reason).

        Args:
            type (str):  Alarm type
            source (str|int):  Database ID of a source device
            fragment (str):  Name of a present custom/standard fragment
            value (str):  Type/Name of a present value fragment
            series (str):  Name of a present series within a value fragment
            before (datetime|str):  Datetime object or ISO date/time string.
                Only measurements assigned to a time before this date are
                returned.
            after (datetime|str):  Datetime object or ISO date/time string.
                Only measurements assigned to a time after this date are
                returned.
            min_age (timedelta):  Timedelta object. Only measurements of
                at least this age are returned.
            max_age (timedelta):  Timedelta object. Only measurements with
                at most this age are returned.
            reverse (bool):  Invert the order of results, starting with the
                most recent one.
            limit (int):  Limit the number of results to this number.
            page_size (int):  Define the number of measurements which are
                read (and parsed in one chunk). This is a performance
                related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator[Measurement]: Iterable of matching Measurement objects
        """
        base_query = self._build_base_query(type=type, source=source, fragment=fragment,
                                            valueFragmentType=value, valueFragmentSeries=series,
                                            before=before, after=after, min_age=min_age, max_age=max_age,
                                            reverse=reverse, page_size=page_size)
        return super()._iterate(base_query, page_number, limit, Measurement.from_json)

    def get_all(
            self,
            type: str = None,
            source: str | int = None,
            fragment: str = None,
            value: str = None,
            series: str = None,
            before: str | datetime = None,
            after: str | datetime = None,
            min_age: timedelta = None,
            max_age: timedelta = None,
            reverse: bool = None,
            limit: int = None,
            page_size: int = 1000,
            page_number: int = None
    ) -> List[Measurement]:
        """ Query the database for measurements and return the results
        as list.

        This function is a greedy version of the select function. All
        available results are read immediately and returned as list.

        Returns:
            List of matching Measurement objects
        """
        return list(self.select(
            type=type,
            source=source,
            fragment=fragment,
            value=value,
            series=series,
            before=before,
            after=after,
            min_age=min_age,
            max_age=max_age,
            reverse=reverse,
            limit=limit,
            page_size=page_size,
            page_number=page_number))

    def get_last(
            self,
            type: str = None,
            source: str | int = None,
            fragment: str = None,
            value: str = None,
            series: str = None,
            before: str | datetime = None,
            min_age: timedelta = None
    ) -> Measurement:
        """ Query the database and return the last matching measurement.

        This function is a special variant of the select function. Only
        the last matching result is returned.

        Returns:
            Last matching Measurement object
        """
        # at least one date qualifier is required for this query to function,
        # so we enforce the 'after' filter if nothing else is specified
        after = None
        if not before and not min_age:
            after = '1970-01-01'
        base_query = self._build_base_query(
            type=type,
            source=source,
            fragment=fragment,
            value=value,
            series=series,
            after=after,
            before=before,
            min_age=min_age,
            reverse=True,
            page_size=1)
        m = Measurement.from_json(self._get_page(base_query, page_number=1)[0])
        m.c8y = self.c8y  # inject c8y connection into instance
        return m

    def get_series(
            self,
            source: str = None,
            aggregation: str = None,
            series: str | Sequence[str] = None,
            before: str | datetime = None,
            after: str | datetime = None,
            min_age: timedelta = None,
            max_age: timedelta = None,
            reverse=False) -> Series:
        """Query the database for a list of series and their values.

        Args:
            source (str):  Database ID of a source device
            aggregation (str):  Aggregation type
            series (str|Sequence[str]):  Series' to query
            before (datetime|str):  Datetime object or ISO date/time string.
                Only measurements assigned to a time before this date are
                included.
            after (datetime|str):  Datetime object or ISO date/time string.
                Only measurements assigned to a time after this date are
                included.
            min_age (timedelta):  Timedelta object. Only measurements of
                at least this age are included.
            max_age (timedelta):  Timedelta object. Only measurements with
                at most this age are included.
            reverse (bool):  Invert the order of results, starting with the
                most recent one.

        Returns:
            A Series object which wraps the raw JSON result but can also be
            used to conveniently collect the series' values.

        See also: https://cumulocity.com/api/core/#operation/getMeasurementSeriesResource
        """
        params = self._prepare_query_params(
            source=source,
            # this is a non-mapped parameter
            aggregationType=aggregation,
            before=before,
            after=after,
            min_age=min_age,
            max_age=max_age,
            reverse=reverse)
        # The 'series' parameter has to be added manually; because it
        # may be a list and because 'series' is by default converted to
        # the 'valueFragmentSeries' parameter
        if series:
            params['series'] = series
        # Build the URL, ensuring that the 'series' parameter is properly
        # expanded in case it is a list.
        url = self.resource + '/series?' + urlencode(params, doseq=True)
        series_json = self.c8y.get(url)
        return Series(series_json)

    def delete_by(
            self,
            type: str = None,
            source: str | int = None,
            fragment: str = None,
            value: str = None,
            series: str = None,
            before: str | datetime = None,
            after: str | datetime = None,
            min_age: timedelta = None,
            max_age: timedelta = None,
        ):
        """ Query the database and delete matching measurements.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification.  Filters can be
        combined (within reason).

        Args: See 'select' function
        """
        base_query = self._build_base_query(
            type=type,
            source=source,
            fragment=fragment,
            value=value,
            series=series,
            before=before,
            after=after,
            min_age=min_age,
            ax_age=max_age)
        self.c8y.delete(base_query)

    # delete function is defined in super class

    def create(self, *measurements):
        """ Bulk create a collection of measurements within the database.

        Args:
            *measurements (Measurement): Collection of Measurement objects.
        """
        self._create_bulk(Measurement.to_json, 'measurements', self.c8y.CONTENT_MEASUREMENT_COLLECTION, *measurements)
