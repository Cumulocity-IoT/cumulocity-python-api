# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=missing-function-docstring

import math
from datetime import datetime, timedelta
import logging
from random import random

from dotenv import load_dotenv
from inputimeout import inputimeout, TimeoutOccurred
import pandas as pd

from c8y_api.app import SimpleCumulocityApp
from c8y_api.model import Device, Measurement, Measurements, Count

logging.basicConfig(level=logging.DEBUG)

load_dotenv()  # load environment from a .env if present
c8y = SimpleCumulocityApp()
print("CumulocityApp initialized.")
print(f"{c8y.base_url}, Tenant: {c8y.tenant_id}, User:{c8y.username}")


# Creating a new (digital only) device to play with
new_device = Device(c8y, type='cx_SomeDevice', name='MyTestDevice',
                    c8y_SupportedSeries=['cx_Data.cx_valueA', 'cx_Data.cx_valueB', 'c8y_Counter.iteration']).create()
print(f"\nCreated new device: {new_device.name} #{new_device.id}")


# Creating measurements
n = 1000
start_datetime = datetime.fromisoformat('2020-01-01 00:00:00.000+00:00')
time_gap = timedelta(seconds=20)

def create_DataMeasurement(seed):
    a = math.sin(seed + random() * 0.2)
    b = math.cos(seed + random() * 0.2)
    # the measurement's values are provided as custom fragments,
    # (here: cx_Data). The JSON structure must be like illustrated below
    return Measurement(type='cx_DataMeasurement', source=new_device.id,
                       time=start_datetime + seed * time_gap,
                       cx_Data={'A': {'value': a, 'unit': 'as'},
                                'B': {'value': b, 'unit': 'bs'}})

def create_CounterMeasurement(seed):
    # The measurement's values are provided as custom fragments,
    # (here: c8y_Counter). There are helper classes available to build
    # the required JSON structure (here Count but there are others like
    # Meters, Liters, Kilograms).
    return Measurement(type='cx_CounterMeasurement', source=new_device.id,
                       time=start_datetime + seed * time_gap,
                       c8y_Counter={'iteration': Count(seed)})

# prepare measurements
ms = [create_CounterMeasurement(i) for i in range(1000)] +\
     [create_DataMeasurement(i) for i in range(1000)]

# create in bulk
c8y.measurements.create(*ms)


# Querying measurements directly
# a) by series
data_measurements = c8y.measurements.get_all(source=new_device.id, after=start_datetime, type='cx_DataMeasurement')
a_values = [m.cx_Data.A.value for m in data_measurements]
b_values = [m.cx_Data.B.value for m in data_measurements]
assert len(a_values) == len(b_values)
# b) by type, including timestamps
counter_measurements = c8y.measurements.get_all(source=new_device.id, after=start_datetime, series='iteration')
i_values = [(m.time, m.c8y_Counter.iteration.value) for m in counter_measurements]

df = pd.DataFrame(data={'a': a_values, 'b': b_values})
df[['time', 'count']] = i_values
print(df.head())

# Querying series
series_result = c8y.measurements.get_series(source=new_device.id, series=['cx_Data.A', 'cx_Data.B'],
                                            aggregation=Measurements.AggregationType.MINUTELY,
                                            after=start_datetime, before='now')
data = series_result.collect(value='min', timestamps='datetime')
df2 = pd.DataFrame(data=data, columns=['timestamp', *[s.name for s in series_result.specs]])
print(df2.head())

# Cleaning up
print("\n\nCleanup:\n")

wait_time = 10
try:
    inputimeout(f"Press ENTER to continue. (Timeout: {wait_time}s)", timeout=wait_time)
except TimeoutOccurred:
    pass

new_device.delete()
print('\nDevice removed.')
