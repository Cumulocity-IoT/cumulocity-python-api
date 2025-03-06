# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

import re

import numpy as np
import pandas as pd

from c8y_api.model import Series


def encode(n):
    """Encode a column name."""
    return re.sub(r'[ \\.+-]', '_', n)


def to_numpy(data: Series, series: str | list[str] = None, value: str = None, timestamps: bool | str = None):
    """Build a NumPy array from a Cumulocity Series object.

    This functions extracts the min and/or max values or one or multiple
    series define within the Series object.
    The result is either a 1-dimensional array if only a single series
    and value is extracted or a 2-dimensional array if either multiple
    series and/or multiple values are extracted.

    The arrays 'columns' are ordered as defined via the `series` argument
    or as defined in the source Series object. If both min and max values
    are extracted, they will be grouped adjacent to each other in the result.

    If the `timestamps` argument is set, the result is a tuple of two NumPy
    arrays; the first holding the data the second the isolated timestamps
    as 1-dimensional array.

    Args:
        data (Series):  A c8y_api Series object
        series (str|list):  A series' name or a collection of series names;
            If omitted, all available series are extracted.
        value (str):  The value (min/max) to extract; If omitted, both min
            and max will be extracted.
        timestamps (bool|str):  Whether to extract the series timestamps;
            If True, the timestamp strings will be used and this function
            returns a tuple (data, timestamps); Use 'datetime' or 'epoch'
            to parse the timestamp strings.

    Returns:
        A NumPy array or a 2-tuple of NumPy arrays if timestamps are included.
    """
    collected = data.collect(series=series, value=value, timestamps=timestamps)

    # handle empty result separately
    if not collected:
        return np.empty(0) if not timestamps else (np.empty(0), np.empty(0))

    # extract timestamps if requested
    if timestamps:
        timestamps = [x[0] for x in collected]
        collected = [x[1:] for x in collected]

    # if there are multiple series and both min/max values are collected,
    # we need to flatten these min/max tuples; timestamps are separate
    array = np.array(collected)
    if array.ndim > 1:
        if array.ndim == 2 and array.shape[1] == 1:
            array = array.reshape(len(collected))
        else:
            array = array.reshape(len(collected), -1)

    # timestamps cannot be part of the result as they may have a different
    # data type (string or datetime), hence we return a tuple in this case
    if timestamps:
        if timestamps == 'datetime':
            timestamps = pd.to_datetime(timestamps)
        return array, np.array(timestamps)

    return array


def to_data_frame(data: Series, series: str | list[str] = None, value: str = None,
                  timestamps: bool | str = None):
    """Build a Pandas DataFrame from a Cumulocity Series object.

    Args:
        data (Series):  A c8y_api Series object
        series (str|list):  A series' name or a collection of series names;
            If omitted, all available series are extracted. The series names
            will be used as column names (special characters will be replaced)
        value (str):  The value (min/max) to extract; If omitted, both min
            and max will be extract and the column names will be suffixed
            accordingly.
        timestamps (bool|str):  Whether to extract the series timestamps as
            index; If True, the timestamp string will be used; Use 'datetime'
            or 'epoch' to parse the timestamp string.

    Returns:
        A Pandas DataFrame object.
    """

    def assemble_column_names():
        names = series if not isinstance(series, str) else [series]
        encoded_names = [encode(n) for n in names]
        # we don't append min/max suffixes if there is only one column
        if isinstance(value, str):
            return encoded_names
        return [f'{n}_{v}' for n in encoded_names for v in ['min', 'max']]

    if not series:
        series = [s.series for s in data.specs]

    columns = assemble_column_names()

    if timestamps:
        array, array_ts = to_numpy(data, series=series, value=value, timestamps=timestamps)
    else:
        array, array_ts = to_numpy(data, series=series, value=value), None

    return pd.DataFrame(data=array, columns=columns, index=array_ts)


def to_series(data: Series, series: str = None, value: str = 'min', timestamps: bool | str = None):
    """Build a Pandas Series from a Cumulocity Series object.

    Args:
        data (Series):  A c8y_api Series object
        series (str):  A series' name; can be left blank if `data` holds only
            the values of one series
        value (str):  The value (min/max) to extract; defaults to 'min'
        timestamps (bool|str):  Whether to extract the series' timestamps as
            index; If True, the timestamp string will be used; Use 'datetime'
            or 'epoch' to parse the timestamp string.

    Returns:
        A Pandas Series object.
    """

    if not series:
        series = [s.series for s in data.specs]
        if len(data.specs) > 1:
            raise ValueError(f"Multiple potential series found ({', '.join(series)}).")
        series = series[0]

    if timestamps:
        array, array_ts = to_numpy(data, series=series, value=value, timestamps=timestamps)
    else:
        array, array_ts = to_numpy(data, series=series, value=value), None

    return pd.Series(array, index=array_ts, name=encode(series))
