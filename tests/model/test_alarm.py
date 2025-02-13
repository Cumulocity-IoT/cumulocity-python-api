# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=redefined-outer-name

from __future__ import annotations

import json
import os
from datetime import datetime
from unittest.mock import Mock

import pytest

from c8y_api.model import Alarm
from tests.utils import isolate_last_call_arg


@pytest.fixture(scope='function')
def sample_alarm() -> Alarm:
    """Provide a sample object for various tests."""
    return Alarm(type='type', text='text', time='2020-01-31T22:33:44Z', source='12345',
                 status='ACTIVE', severity='MAJOR',
                 simple_string='string',
                 simple_int=123,
                 simple_float=123.4,
                 simple_true=True,
                 simple_false=False,
                 complex_1={'level0': 'value'},
                 complex_2={'string': 'value', 'level0': {'level1': 'value'}})


def test_parsing():
    """Verify that parsing an Alarm from JSON works."""
    path = os.path.dirname(__file__) + '/alarm.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        alarm_json = json.load(f)
    event = Alarm.from_json(alarm_json)

    assert event.id == alarm_json['id']
    assert event.type == alarm_json['type']
    assert event.text == alarm_json['text']
    assert event.source == alarm_json['source']['id']
    assert event.time == alarm_json['time']
    assert event.creation_time == alarm_json['creationTime']

    assert isinstance(event.datetime, datetime)
    assert isinstance(event.creation_datetime, datetime)

    assert event.custom_attribute == 'value'
    assert event.custom_fragment.test.string == 'string'
    assert event.custom_fragment.test.false is False


def test_default_values():
    """Verify that the full JSON is enriched with creation defaults."""
    # 1) create a minimal Alarm instance
    alarm = Alarm(type='type', source='123', text='text', severity='MAJOR')

    # -> status is not set
    assert not alarm.status
    # -> time is not set
    assert not alarm.time

    alarm_json = alarm.to_full_json()

    # -> status should not be defaulted
    assert 'status' not in alarm_json
    # -> time should not be set in the full JSON
    assert 'time' not in alarm_json

    # 2) invoking the create function
    alarm.c8y = Mock()
    alarm.c8y.post = Mock()
    alarm.from_json = Mock()  # mocking this as the post result will be crap
    alarm.create()
    # -> posted JSON should not contain a time as it is not set in the object
    assert 'time' not in isolate_last_call_arg(alarm.c8y.post, 'json', 1)


def test_formatting(sample_alarm: Alarm):
    """Verify that JSON formatting works."""
    sample_alarm.id = 'id'
    alarm_json = sample_alarm.to_full_json()

    assert 'id' not in alarm_json
    assert 'creationTime' not in alarm_json
    assert 'firstOccurrenceTime' not in alarm_json

    assert alarm_json['type'] == sample_alarm.type
    assert alarm_json['source']['id'] == sample_alarm.source
    assert alarm_json['text'] == sample_alarm.text
    assert alarm_json['time'] == sample_alarm.time
    assert alarm_json['severity'] == sample_alarm.severity
    assert alarm_json['status'] == sample_alarm.status

    assert alarm_json['simple_string'] == sample_alarm.simple_string
    assert alarm_json['simple_int'] == sample_alarm.simple_int
    assert alarm_json['simple_float'] == sample_alarm.simple_float
    assert alarm_json['simple_true'] is True
    assert alarm_json['simple_false'] is False
    assert alarm_json['complex_1']['level0'] == 'value'
    assert alarm_json['complex_2']['level0']['level1'] == 'value'

    expected_keys = {'type', 'text', 'time', 'source', 'severity', 'status',
                     'simple_string', 'simple_int', 'simple_float', 'simple_true', 'simple_false',
                     'complex_1', 'complex_2'}
    assert set(alarm_json.keys()) == expected_keys


def test_now_datetime():
    """Verify that by default the current datetime will be applied."""
    alarm = Alarm(None, 'type', time='now', source='12345')

    assert alarm.time
    assert 'time' in alarm.to_full_json()
