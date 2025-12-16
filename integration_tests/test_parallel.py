from datetime import datetime, timedelta

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import Measurement, Count, Device
from c8y_tk.analytics.parallel import ParallelExecutor


@pytest.fixture(name='sample_measurements', scope='session')
def fix_measurements(live_c8y: CumulocityApi, session_device: Device):
    """Create a bucketload of measurements on a test device."""
    n = 100

    start = datetime.fromisoformat('2020-01-01T00:00:00Z')
    interval = timedelta(hours=24) / n

    ms = [
        Measurement(
            type='c8y_TestMeasurement',
            source=session_device.id,
            time=(start + i*interval),
            **{'testSeries': {'testValue': Count(i)}}
        )
        for i in range(n)
    ]
    live_c8y.measurements.create(*ms)

    return ms


@pytest.mark.parametrize('workers, page_size', [
    (1, 10),
    (10, 10),
    (1, 100),
    (10, 100),
    (100, 10),
])
def test_parallel(live_c8y:CumulocityApi, sample_measurements, workers, page_size) -> None:
    """Verify that parallel execution works as expected."""

    device_id = sample_measurements[0].source
    start = sample_measurements[0].time
    end = sample_measurements[-1].datetime + timedelta(minutes=1)

    series = 'testSeries.testValue.value'

    kwargs = {
        'source': device_id,
        'page_size': page_size,
        'after': start,
        'before': end,
        'as_values': series,
    }

    ms1 = live_c8y.measurements.get_all(**kwargs)
    ms2 = ParallelExecutor.as_list(live_c8y.measurements, workers=workers, strategy='pages', **kwargs)

    assert len(ms1) == len(ms2)


def test_parallel_execution(live_c8y: CumulocityApi) -> None:

    devices = live_c8y.device_inventory.get_all(limit=10)

    with ParallelExecutor(5) as executor:
        last_measurements = executor.parallel(
            [lambda: live_c8y.measurements.get_last(source=x.id) for x in devices]
        ).as_list()

    assert len(last_measurements) == len(devices)


