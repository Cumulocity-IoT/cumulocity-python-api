# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=redefined-outer-name

from __future__ import annotations

import json
import os
from datetime import datetime

import pytest

from c8y_api.model import Event


@pytest.fixture(scope='function')
def sample_event() -> Event:
    """Provide a sample object for various tests."""
    return Event(type='type', text='text', time='2020-01-31T22:33:44Z', source='12345',
                 simple_string='string',
                 simple_int=123,
                 simple_float=123.4,
                 simple_true=True,
                 simple_false=False,
                 complex_1={'level0': 'value'},
                 complex_2={'string': 'value', 'level0': {'level1': 'value'}})


def test_parsing():
    """Verify that parsing an Event from JSON works."""
    path = os.path.dirname(__file__) + '/event.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        event_json = json.load(f)
    event = Event.from_json(event_json)

    assert event.id == event_json['id']
    assert event.type == event_json['type']
    assert event.text == event_json['text']
    assert event.source == event_json['source']['id']
    assert event.time == event_json['time']
    assert event.creation_time == event_json['creationTime']

    assert isinstance(event.datetime, datetime)
    assert isinstance(event.creation_datetime, datetime)

    assert event.custom_attribute == 'value'
    assert event.custom_fragment.test.string == 'string'
    assert event.custom_fragment.test.false is False


def test_formatting(sample_event: Event):
    """Verify that JSON formatting works."""
    sample_event.id = 'id'
    event_json = sample_event.to_full_json()

    assert 'id' not in event_json
    assert 'creationTime' not in event_json

    assert event_json['type'] == sample_event.type
    assert event_json['source']['id'] == sample_event.source
    assert event_json['text'] == sample_event.text
    assert event_json['time'] == sample_event.time

    assert event_json['simple_string'] == sample_event.simple_string
    assert event_json['simple_int'] == sample_event.simple_int
    assert event_json['simple_float'] == sample_event.simple_float
    assert event_json['simple_true'] is True
    assert event_json['simple_false'] is False
    assert event_json['complex_1']['level0'] == 'value'
    assert event_json['complex_2']['level0']['level1'] == 'value'

    expected_keys = {'type', 'text', 'time', 'source',
                     'simple_string', 'simple_int', 'simple_float', 'simple_true', 'simple_false',
                     'complex_1', 'complex_2'}
    assert set(event_json.keys()) == expected_keys


def test_updating(sample_event: Event):
    """Verify that updating results in proper diff JSON."""

    # 1) after no update
    assert not sample_event.get_updates()
    event_json = sample_event.to_diff_json()
    assert event_json == {}

    # 2) readonly properties are not recorded
    sample_event.id = 'id'
    sample_event.type = 'new type'
    sample_event.time = '2001-12-31'
    sample_event.creation_time = '2001-12-31'
    sample_event.source = 'new source'
    assert not sample_event.get_updates()
    assert sample_event.to_diff_json() == {}

    # 3) updatable properties are recorded
    sample_event.text = 'new text'
    expected_updates = {'text'}
    # -> len is the same, we cannot test the keys as they are internal
    assert len(sample_event.get_updates()) == len(expected_updates)
    assert set(sample_event.to_diff_json().keys()) == expected_updates

    # 4) updated fragments are recorded
    # Note: simple fragments can only be updated using [] notation
    sample_event['simple_float'] = 543.21
    sample_event['simple_false'] = False
    sample_event.complex_2.level0.level1 = 'new value'
    expected_updates.update({'simple_float', 'simple_false', 'complex_2'})
    assert len(sample_event.get_updates()) == len(expected_updates)
    assert set(sample_event.to_diff_json().keys()) == expected_updates


def test_now_datetime():
    """Verify that by default the current datetime will be applied."""
    event = Event(None, type='type', time='now')

    assert event.time
    assert 'time' in event.to_full_json()
