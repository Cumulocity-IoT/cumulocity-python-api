
from datetime import datetime as dt,  timedelta as td, timezone as tz

import numpy as np
from pandas import DatetimeIndex, RangeIndex

from unittest.mock import patch, Mock
import pytest

from c8y_api.model import Series
from c8y_tk.analytics import to_data_frame, to_numpy, to_series


@pytest.mark.parametrize('names, values, expected',[
    (['type'], ['min'], ['type']),
    (['type.name'], ['max'], ['type_name']),
    (['type.name'], ['min', 'max'], ['type_name_min', 'type_name_max']),
    (['one', 'two'], ['min'], ['one', 'two']),
    (['one', 'two'], ['min', 'max'], ['one_min', 'one_max', 'two_min', 'two_max']),
    (['one name', 'two'], ['min', 'max'], ['one_name_min', 'one_name_max', 'two_min', 'two_max']),
    (['a.b c_d', 'a_1.b_.c'], ['x'], ['a_b_c_d', 'a_1_b__c']),
], ids=[
    'simple',
    'dot',
    'dot_min_max',
    'multi',
    'multi_space',
    'multi_mix_min_max',
    'characters',
])
@pytest.mark.skip("No longer necessary?")
def test_build_columns(names, values, expected):
    """Verify that column name encoding works as expected."""
    pass


@pytest.fixture(name='sample_series')
def fix_sample_series():
    """Create sample series (a single and a multi series as tuple.)"""
    length = 5

    # create some timestamps
    start_datetime = dt.now(tz=tz.utc)
    timestamps = [(start_datetime + i * td(hours=1)).isoformat() for i in range(length)]
    # create series of increasing min/max values for each timestamp
    # where max is a little bigger than min, a = 10s, b=20s, c=30s
    a, b, c = (
        [(b + i, b + 0.5 + i) for i in range(length)] for b in [10, 20, 30]
    )

    def create_series(values):
        """Create Series JSON from series values."""
        return {
            'values': {
                    t: [
                        {'min': v[i][0], 'max': v[i][1]}
                        for v in values
                    ] for i, t in enumerate(timestamps)
            },
            'series': [
                {
                    'unit': '#',
                    'name': f'name{i}',
                    'type': f'type{i}'
                } for i in range(1, len(values)+1)
            ],
            'truncated': 'false',
        }

    single_series = create_series([a])
    multi_series = create_series([a, b, c])

    # just a print for debugging in case of failures
    import json
    print("Series A:")
    print(json.dumps(single_series, indent=3))
    print("Series A, B, C:")
    print(json.dumps(multi_series, indent=3))

    return Series(single_series), Series(multi_series)


@pytest.fixture(name='single_series')
def fix_single_series(sample_series):
    """Provide a single series."""
    return sample_series[0]


@pytest.fixture(name='multi_series')
def fix_multi_series(sample_series):
    """Provide a multi series."""
    return sample_series[1]


def test_to_numpy_single_series(single_series):
    """Verify that a single-series NumPy array is build correctly from raw series data."""
    timestamps = list(single_series['values'].keys())
    datetimes = [dt.fromisoformat(x) for x in timestamps]
    min_values = [x[0]['min'] for x in single_series['values'].values()]
    max_values = [x[0]['max'] for x in single_series['values'].values()]

    # create an array of min values
    array = to_numpy(single_series, value='min')
    assert min_values == array.tolist()

    # create an array of min values (named)
    array = to_numpy(single_series, series=single_series.specs[0].series, value='min')
    assert min_values == array.tolist()

    # create an array of min values (named list)
    array = to_numpy(single_series, series=[single_series.specs[0].series], value='min')
    assert min_values == array.tolist()

    # create an array of min values with timestamps
    array, array_ts = to_numpy(single_series, value='min', timestamps=True)
    assert min_values == array.tolist()
    assert timestamps == array_ts.tolist()

    # create an array of min values with datetime timestamps
    array, array_ts = to_numpy(single_series, value='min', timestamps='datetime')
    assert min_values == array.tolist()
    assert datetimes == array_ts.tolist()

    # create an array of min/max values
    array = to_numpy(single_series)
    assert min_values == array[:, 0].tolist()
    assert max_values == array[:, 1].tolist()

    # create an array of min/max values with timestamps
    array, array_ts = to_numpy(single_series, timestamps=True)
    assert min_values == array[:, 0].tolist()
    assert max_values == array[:, 1].tolist()
    assert timestamps == array_ts.tolist()


def test_to_numpy_multi_series(multi_series):
    """Verify that a multi-series NumPy array is build correctly from raw series data."""
    timestamps = list(multi_series['values'].keys())
    datetimes = [dt.fromisoformat(x) for x in timestamps]
    min_values_a, min_values_b, min_values_c = (
        [x[i]['min'] for x in  multi_series['values'].values()]
        for i in range(3)
    )
    max_values_a, max_values_b, max_values_c = (
        [x[i]['max'] for x in  multi_series['values'].values()]
        for i in range(3)
    )
    series_name_a, series_name_b, series_name_c = (
        multi_series.specs[i].series
        for i in range(3)
    )

    # create a single array of min values
    array = to_numpy(multi_series, series=series_name_a, value='min')
    assert min_values_a == array.tolist()

    # create a single array of min values (from list)
    array = to_numpy(multi_series, series=[series_name_a], value='min')
    assert min_values_a == array.tolist()

    # create a multi array of min values
    array = to_numpy(multi_series, series=[series_name_b, series_name_c], value='min')
    assert min_values_b == array[:, 0].tolist()
    assert min_values_c == array[:, 1].tolist()

    # create a single array of min values and timestamps
    array, array_ts = to_numpy(multi_series, series=series_name_a, value='min', timestamps=True)
    assert min_values_a == array.tolist()
    assert timestamps == array_ts.tolist()

    # create a single array of min values and datetime timestamps
    array, array_ts = to_numpy(multi_series, series=series_name_a, value='min', timestamps='datetime')
    assert min_values_a == array.tolist()
    assert datetimes == array_ts.tolist()

    # create a multi array of min values with timestamps
    array, array_ts = to_numpy(multi_series, series=[series_name_b, series_name_c], value='min', timestamps=True)
    assert min_values_b == array[:, 0].tolist()
    assert min_values_c == array[:, 1].tolist()
    assert timestamps == array_ts.tolist()

    # create a single array of min/max values
    array = to_numpy(multi_series, series=series_name_a)
    assert min_values_a == array[:, 0].tolist()
    assert max_values_a == array[:, 1].tolist()

    # create a single array of min/max values (from list)
    array = to_numpy(multi_series, series=[series_name_a])
    assert min_values_a == array[:, 0].tolist()
    assert max_values_a == array[:, 1].tolist()

    # create a multi array of min/max values
    array = to_numpy(multi_series, series=[series_name_a, series_name_c])
    assert min_values_a == array[:, 0].tolist()
    assert max_values_a == array[:, 1].tolist()
    assert min_values_c == array[:, 2].tolist()
    assert max_values_c == array[:, 3].tolist()

    # create a single array of min/max values with timestamps
    array, array_ts = to_numpy(multi_series, series=series_name_a, timestamps=True)
    assert min_values_a == array[:, 0].tolist()
    assert max_values_a == array[:, 1].tolist()
    assert timestamps == array_ts.tolist()

    # create a single array of min/max values from list with timestamps
    array, array_ts = to_numpy(multi_series, series=[series_name_b], timestamps=True)
    assert min_values_b == array[:, 0].tolist()
    assert max_values_b == array[:, 1].tolist()
    assert timestamps == array_ts.tolist()

    # create a multi array of min/max values with timestamps
    array, array_ts = to_numpy(multi_series, series=[series_name_a, series_name_c], timestamps=True)
    assert min_values_a == array[:, 0].tolist()
    assert max_values_a == array[:, 1].tolist()
    assert min_values_c == array[:, 2].tolist()
    assert max_values_c == array[:, 3].tolist()
    assert timestamps == array_ts.tolist()

    # create a multi array of min/max values with timestamps
    array, array_ts = to_numpy(multi_series, series=[series_name_a, series_name_c], timestamps='datetime')
    assert min_values_a == array[:, 0].tolist()
    assert max_values_a == array[:, 1].tolist()
    assert min_values_c == array[:, 2].tolist()
    assert max_values_c == array[:, 3].tolist()
    assert datetimes == array_ts.tolist()


def test_to_data_frame_from_numpy():
    """Verify that a DataFrame is correctly build from NumPy arrays."""
    series = Mock()
    series.specs = [Series.SeriesSpec('#', 'type', 'name')]
    series_name = series.specs[0].series

    with patch('c8y_tk.analytics._wrappers.to_numpy') as to_numpy_mock:
        to_numpy_mock.return_value = np.array([1, 2, 3])
        df = to_data_frame(data=series, series=series_name, value='min')
        assert df.columns == ['type_name']
        assert df['type_name'].to_list() == [1, 2, 3]

    with patch('c8y_tk.analytics._wrappers.to_numpy') as to_numpy_mock:
        values = [[10, 11], [20, 21], [30, 31]]
        to_numpy_mock.return_value = np.array(values)
        df = to_data_frame(data=series, series=series_name)
        assert df.columns.to_list() == ['type_name_min', 'type_name_max']
        assert df[df.columns[0]].to_list() == [v[0] for v in values]
        assert df[df.columns[1]].to_list() == [v[1] for v in values]

    with patch('c8y_tk.analytics._wrappers.to_numpy') as to_numpy_mock:
        values = [1, 2, 3]
        timestamps = ['a', 'b', 'c']
        to_numpy_mock.return_value = (np.array(values), np.array(timestamps))
        df = to_data_frame(data=series, series=series_name, value='min', timestamps=True)
        assert df.columns == ['type_name']
        assert df['type_name'].to_list() == values
        assert df.index.to_list() == timestamps

    with patch('c8y_tk.analytics._wrappers.to_numpy') as to_numpy_mock:
        values = [[10, 11], [20, 21], [30, 31]]
        timestamps = ['a', 'b', 'c']
        to_numpy_mock.return_value = (np.array(values), np.array(timestamps))
        df = to_data_frame(data=series, series=[series_name], timestamps=True)
        assert df.columns.to_list() == ['type_name_min', 'type_name_max']
        assert df['type_name_min'].to_list() == [v[0] for v in values]
        assert df['type_name_max'].to_list() == [v[1] for v in values]
        assert df.index.to_list() == timestamps


def test_to_data_frame_from_numpy_multi():
    """Verify that a DataFrame is correctly build from NumPy arrays."""
    series = Mock()
    series.specs = [
        Series.SeriesSpec('#', 'type1', 'name1'),
        Series.SeriesSpec('#', 'type2', 'name2')
    ]
    series_names = [s.series for s in series.specs]

    # multi series, single value, including timestamps
    with patch('c8y_tk.analytics._wrappers.to_numpy') as to_numpy_mock:
        values = [[10, 11], [20, 21], [30, 31]]
        timestamps = ['a', 'b', 'c']
        to_numpy_mock.return_value = (np.array(values), np.array(timestamps))
        df = to_data_frame(data=series, series=series_names, value='min', timestamps=True)
        assert df.columns.to_list() == ['type1_name1', 'type2_name2']
        assert df['type1_name1'].to_list() == [v[0] for v in values]
        assert df['type2_name2'].to_list() == [v[1] for v in values]
        assert df.index.to_list() == timestamps

    # multi series, multi value, including timestamps
    with patch('c8y_tk.analytics._wrappers.to_numpy') as to_numpy_mock:
        values = [[10, 11, 110, 111], [20, 21, 120, 121], [30, 31, 130, 131]]
        timestamps = ['a', 'b', 'c']
        to_numpy_mock.return_value = (np.array(values), np.array(timestamps))
        df = to_data_frame(data=series, series=series_names, timestamps=True)
        assert df.columns.to_list() == ['type1_name1_min', 'type1_name1_max', 'type2_name2_min', 'type2_name2_max']
        assert df['type1_name1_min'].to_list() == [v[0] for v in values]
        assert df['type1_name1_max'].to_list() == [v[1] for v in values]
        assert df['type2_name2_min'].to_list() == [v[2] for v in values]
        assert df['type2_name2_max'].to_list() == [v[3] for v in values]
        assert df.index.to_list() == timestamps


def test_to_data_frame_single(single_series):
    """Verify that a DataFrame is build correctly from raw series data."""
    timestamps = list(single_series['values'].keys())
    datetimes = [dt.fromisoformat(x) for x in timestamps]
    min_values = [x[0]['min'] for x in single_series['values'].values()]
    max_values = [x[0]['max'] for x in single_series['values'].values()]
    series_name = single_series.specs[0].series
    column_base = series_name.replace('.', '_')

    # create a data frame of min values
    df = to_data_frame(single_series, value='min')
    # -> should have a single value column
    assert isinstance(df.index, RangeIndex)
    assert df.columns.to_list() == [column_base]
    assert df[column_base].to_list() == min_values

    # create a data frame of min/max values with timestamps
    df = to_data_frame(single_series, series=series_name, timestamps='datetime')
    # -> should have min and max columns
    assert isinstance(df.index, DatetimeIndex)
    assert df.columns.to_list() == [f'{column_base}_min', f'{column_base}_max']
    assert df[f'{column_base}_min'].to_list() == min_values
    assert df[f'{column_base}_max'].to_list() == max_values
    assert df.index.to_list() == datetimes


def test_to_data_frame_multi(multi_series):
    """Verify that a DataFrame is build correctly from raw series data."""
    timestamps = list(multi_series['values'].keys())
    datetimes = [dt.fromisoformat(x) for x in timestamps]
    min_values_a, min_values_b, min_values_c = (
        [x[i]['min'] for x in  multi_series['values'].values()]
        for i in range(3)
    )
    max_values_a, max_values_b, max_values_c = (
        [x[i]['max'] for x in  multi_series['values'].values()]
        for i in range(3)
    )
    series_name_a, series_name_b, series_name_c = (
        multi_series.specs[i].series
        for i in range(3)
    )
    column_base_a, column_base_b, column_base_c = (
        n.replace('.', '_') for n in [series_name_a, series_name_b, series_name_c]
    )

    # create a data frame of min values
    df = to_data_frame(multi_series, value='min')
    # -> should have a single value column for each series
    assert isinstance(df.index, RangeIndex)
    assert df.columns.to_list() == [column_base_a, column_base_b, column_base_c]
    assert df[column_base_a].to_list() == min_values_a
    assert df[column_base_b].to_list() == min_values_b
    assert df[column_base_c].to_list() == min_values_c

    # create a data frame of min/max values, including timestamps
    df = to_data_frame(multi_series, timestamps='datetime')
    # -> should have a single value column for each series
    assert isinstance(df.index, DatetimeIndex)
    assert df.index.to_list() == datetimes
    assert df.columns.to_list() == [
        f'{c}_{m}' for c in [column_base_a, column_base_b, column_base_c] for m in ['min', 'max']
    ]
    assert df[f'{column_base_a}_min'].to_list() == min_values_a
    assert df[f'{column_base_a}_max'].to_list() == max_values_a
    assert df[f'{column_base_b}_min'].to_list() == min_values_b
    assert df[f'{column_base_b}_max'].to_list() == max_values_b
    assert df[f'{column_base_c}_min'].to_list() == min_values_c
    assert df[f'{column_base_c}_max'].to_list() == max_values_c


def test_to_series_single(single_series):
    timestamps = list(single_series['values'].keys())
    datetimes = [dt.fromisoformat(x) for x in timestamps]
    min_values = [x[0]['min'] for x in single_series['values'].values()]
    max_values = [x[0]['max'] for x in single_series['values'].values()]
    series_name = single_series.specs[0].series
    column_base = series_name.replace('.', '_')

    # create a series of min values
    s = to_series(single_series)
    # -> should have a single value column
    assert isinstance(s.index, RangeIndex)
    assert s.name == column_base
    assert s.to_list() == min_values

    # create a series of max values with timestamp index
    s = to_series(single_series, series=series_name, value='max', timestamps='datetime')
    # -> should have a single value column
    assert s.name == column_base
    assert s.to_list() == max_values
    assert s.index.to_list() == datetimes


def test_to_series_multi(multi_series):
    """Verify that a Series is build correctly from raw series data."""
    timestamps = list(multi_series['values'].keys())
    datetimes = [dt.fromisoformat(x) for x in timestamps]
    min_values_a, min_values_b, min_values_c = (
        [x[i]['min'] for x in  multi_series['values'].values()]
        for i in range(3)
    )
    max_values_a, max_values_b, max_values_c = (
        [x[i]['max'] for x in  multi_series['values'].values()]
        for i in range(3)
    )
    series_name_a, series_name_b, series_name_c = (
        multi_series.specs[i].series
        for i in range(3)
    )
    column_base_a, column_base_b, column_base_c = (
        n.replace('.', '_') for n in [series_name_a, series_name_b, series_name_c]
    )

    # attempt to create from multi series
    with pytest.raises(ValueError) as error:
        to_series(multi_series)
    # -> error message should list potential series names
    assert all(x in str(error) for x in [series_name_a, series_name_b, series_name_c])

    # create a series of min values
    s = to_series(multi_series, series=series_name_a)
    # -> should have a single value column
    assert s.name == column_base_a
    assert s.to_list() == min_values_a
    assert isinstance(s.index, RangeIndex)

    # create a series of max values, with timestamp index
    s = to_series(multi_series, series=series_name_b, value='max', timestamps='datetime')
    # -> should have a single value column
    assert s.name == column_base_b
    assert s.to_list() == max_values_b
    assert isinstance(s.index, DatetimeIndex)
    assert s.index.to_list() == datetimes
