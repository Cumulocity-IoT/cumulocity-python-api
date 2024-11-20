# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

import random
from datetime import datetime, timedelta, timezone
from dateutil import tz
import logging
import time
from typing import List

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Device, Measurement, Measurements, Series, Value, Kelvin, Count

from util.testing_util import RandomNameGenerator


def get_ids(ms: List[Measurement]) -> List[str]:
    """Isolate the ID from a list of measurements."""
    return [m.id for m in ms]


@pytest.fixture(scope='session', name='measurement_factory')
def fix_measurement_factory(live_c8y: CumulocityApi):
    """Provide a factory function to create measurements that are cleaned
    up after the session if needed."""

    created_devices = []

    def factory_fun(n: int, device=None, type=None, series=None) -> List[Measurement]:
        type = type or RandomNameGenerator.random_name(2)
        series = series or type

        # 1) create device
        if not device:
            device = Device(c8y=live_c8y, type=f'{type}_device', name=type, test_marker={'name': type}).create()
            created_devices.append(device)
            logging.info(f'Created device #{device.id}')

        # 2) create measurements
        ms = []
        now = time.time()
        for i in range(0, n):
            measurement_time = datetime.fromtimestamp(now - i*60, tz.tzutc())
            m = Measurement(c8y=live_c8y, type=type, source=device.id, time=measurement_time)
            # m[series] = {series: Value(random.randint(1000, 9999), '#')}
            m[series] = {'series': Value(random.randint(1000, 9999), '#')}
            m['marker'] = {'id': f'{device.id}_{type}_{series}_{i}'}
            m = m.create()
            logging.info(f'Created measurement #{m.id}: {m.to_json()}')
            ms.append(m)
        return ms

    yield factory_fun

    for d in created_devices:
        try:
            d.delete()
        except KeyError:
            logging.warning(f"Device #{d.id} already deleted.")


def test_select(live_c8y: CumulocityApi, measurement_factory):
    """Verify that selection works as expected."""
    # pylint: disable=too-many-statements)
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True

    name = RandomNameGenerator.random_name(2)
    other_name = f'other_{name}'

    # create a couple of measurements (at a new device)
    created_ms = measurement_factory(10, type=name, series=name)

    # create a couple of measurements with different source
    source_ms = measurement_factory(10, type=name, series=name)

    # create a couple of measurements with different type name
    device_id = created_ms[0].source
    device = live_c8y.device_inventory.get(created_ms[0].source)
    type_ms = measurement_factory(10, device=device, type=other_name, series=name)

    # create a couple of measurements with different series name
    series_ms = measurement_factory(10, device=device, type=name, series=other_name)

    # (1) all measurement collections can be selected separately

    # select by source
    same_source_ms = live_c8y.measurements.get_all(source=device_id)
    assert len({x.source for x in same_source_ms}) == 1
    assert len({x.type for x in same_source_ms}) == 2
    assert len({x.get_series()[0] for x in same_source_ms}) == 2
    assert len(same_source_ms) == len(created_ms) + len(type_ms) + len(series_ms)

    # select by type
    same_type_ms = live_c8y.measurements.get_all(type=name)
    assert len({x.source for x in same_type_ms}) == 2
    assert len({x.type for x in same_type_ms}) == 1
    assert len({x.get_series()[0] for x in same_type_ms}) == 2
    assert len(same_type_ms) == len(created_ms) + len(source_ms) + len(series_ms)

    # select by series
    same_series_ms = live_c8y.measurements.get_all(value_fragment_type=name)
    assert len({x.source for x in same_series_ms}) == 2
    assert len({x.type for x in same_series_ms}) == 2
    assert len({x.get_series()[0] for x in same_series_ms}) == 1
    assert len(same_series_ms) == len(created_ms) + len(source_ms) + len(type_ms)

    # (2) Testing deletion

    # Delete all with same source and type (fragment is not supported)
    # This would also include the ones having a different series name
    live_c8y.measurements.delete_by(source=device_id, type=name)
    # wait for the deletion to be executed
    n = 10
    while True:
        if not live_c8y.measurements.get_last(source=device_id, type=name):
            break
        n = n-1
        time.sleep(1 * (10-n))
    assert not live_c8y.measurements.get_last(source=device_id, type=name)

    # -> there should still be similar measurements at a different device
    other_source_ms = live_c8y.measurements.get_all(type=name, value_fragment_type=name)
    assert len(other_source_ms) == len(source_ms)
    # -> there should still be differently typed measurements for the same source
    other_type_ms = live_c8y.measurements.get_all(source=device_id, type=other_name)
    assert len(other_type_ms) == len(type_ms)

    # Delete by type (don't care about the source)
    now = datetime.now(timezone.utc)
    now_truncated = now.replace(hour=now.hour+1, minute=0, second=0, microsecond=0)
    live_c8y.measurements.delete_by(type=name, date_to=now_truncated)
    # wait for the deletion to be executed
    n = 10
    while True:
        if not live_c8y.measurements.get_last(type=name):
            break
        n = n-1
        time.sleep(1 * (10-n))
    assert not live_c8y.measurements.get_last(type=name)

    # -> we should still see some with the other type
    other_type_ms = live_c8y.measurements.get_all(type=other_name, before=now_truncated)
    assert len(other_type_ms) == len(type_ms)

    # Delete remaining measurements
    live_c8y.measurements.delete_by(type=other_name, date_to=now_truncated)
    # wait for the deletion to be executed
    n = 10
    while True:
        if not live_c8y.measurements.get_last(type=other_name):
            break
        n = n-1
        time.sleep(1 * (10-n))
    assert not live_c8y.measurements.get_last(type=other_name)

    # -> no measurements should be left
    sources = [created_ms[0].source, source_ms[1].source]
    for source in sources:
        assert not live_c8y.measurements.get_all(source=source)


def test_single_page_select(live_c8y: CumulocityApi, measurement_factory):
    """Verify that selection works as expected."""
    # create a couple of measurements
    created_ms = measurement_factory(50)
    created_ids = [m.id for m in created_ms]
    device_id = created_ms[0].source

    # select all measurements using different page sizes
    selected_ids = [m.id for m in live_c8y.measurements.select(source=device_id, page_size=10, page_number=2)]

    # -> all created measurements should be in the selection
    assert len(selected_ids) == 10
    assert all(i in set(created_ids) for i in selected_ids)


@pytest.fixture(scope='session', name='sample_series_device')
def fix_sample_series_device(live_c8y: CumulocityApi, sample_device: Device) -> Device:
    """Add measurement series to the sample device."""
    # create 12K measurements, 2 every minute
    start_time = datetime.fromisoformat('2020-01-01 00:00:00+00:00')
    ms_iter = [Measurement(type='c8y_TestMeasurement',
                      source=sample_device.id,
                      time=start_time + (i * timedelta(seconds=30)),
                      c8y_Iteration={'c8y_Counter': Count(i)},
                      ) for i in range(0, 5000)]
    ms_temps = [Measurement(type='c8y_TestMeasurement',
                      source=sample_device.id,
                      time=start_time + (i * timedelta(seconds=100)),
                      c8y_Temperature={'c8y_AverageTemperature': Kelvin(i * 0.2)},
                      ) for i in range(0, 1000)]
    live_c8y.measurements.create(*ms_iter)
    live_c8y.measurements.create(*ms_temps)

    sample_device['c8y_SupportedSeries'] = [
        'c8y_Temperature.c8y_AverageTemperature',
        'c8y_Iteration.c8y_Counter']
    return sample_device.update()


@pytest.fixture(scope='session')
def unaggregated_series_result(live_c8y: CumulocityApi, sample_series_device: Device) -> Series:
    """Provide an unaggregated series result."""
    start_time = datetime.fromisoformat('2020-01-01 00:00:00+00:00')
    return live_c8y.measurements.get_series(source=sample_series_device.id,
                                            series=sample_series_device.c8y_SupportedSeries,
                                            after=start_time, before='now')


@pytest.fixture(scope='session')
def aggregated_series_result(live_c8y: CumulocityApi, sample_series_device: Device) -> Series:
    """Provide an aggregated series result."""
    start_time = datetime.fromisoformat('2020-01-01 00:00:00+00:00')
    return live_c8y.measurements.get_series(source=sample_series_device.id,
                                            series=sample_series_device.c8y_SupportedSeries,
                                            aggregation=Measurements.AggregationType.HOURLY,
                                            after=start_time, before='now')


@pytest.mark.parametrize('series_fixture', [
    'unaggregated_series_result',
    'aggregated_series_result'])
def test_collect_single_series(series_fixture, request):
    """Verify that collecting a single value (min or max) from a
    series works as expected."""
    series_result = request.getfixturevalue(series_fixture)
    for spec in series_result.specs:
        values = series_result.collect(series=spec.series, value='min')
        # -> None values should be filtered out
        assert values
        assert all(v is not None for v in values)
        # -> Values should all have the same type
        # pylint: disable=unidiomatic-typecheck
        assert all(type(a) == type(b) for a, b in zip(values, values[1:]))
        # -> Values should be increasing continuously
        assert all(a<b for a,b in zip(values, values[1:]))


@pytest.mark.parametrize('series_fixture', [
    'unaggregated_series_result',
    'aggregated_series_result'])
def test_collect_multiple_series(series_fixture, request):
    """Verify that collecting a single value (min or max) for multiple
    series works as expected."""
    series_result = request.getfixturevalue(series_fixture)
    series_names = [s.series for s in series_result.specs]
    values = series_result.collect(series=series_names, value='min')
    assert values
    # -> Each element should be an n-tuple (n as number of series)
    assert all(isinstance(v, tuple) for v in values)
    assert all(len(v) == len(series_names) for v in values)
    # -> Each value within the n-tuple belongs to one series
    #    There will be None values (when a series does not define a value
    #    at that timestamp). Subsequent values will have the same type
    assert any(any(e is None for e in v) for v in values)
    for i in range(0, len(series_names)):
        t = type(values[0][i])
        assert all(isinstance(v[i], t) for v in values if v[i])
