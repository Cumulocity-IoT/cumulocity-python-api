# Copyright (c) 2025 Cumulocity GmbH

import json
import os
from datetime import datetime

import pytest

from c8y_api.model import Operation


def test_parsing():
    """Verify that parsing an Operation from JSON works."""
    path = os.path.dirname(__file__) + '/operation.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        operation_json = json.load(f)
    operation = Operation.from_json(operation_json)

    assert operation.id == operation_json['id']
    assert operation.device_id == operation_json['deviceId']
    assert operation.status == operation_json['status']
    assert operation.description == operation_json['description']
    assert operation.creation_time == operation_json['creationTime']

    assert isinstance(operation.creation_datetime, datetime)

    assert operation.c8y_Command.text == operation_json['c8y_Command']['text']


@pytest.fixture(scope='function', name='sample_operation')
def fix_sample_operation() -> Operation:
    """Provide a sample object for various tests."""
    return Operation(device_id='12345', status=Operation.Status.FAILED, description='description text',
                     simple_string='string',
                     simple_int=123,
                     simple_float=123.4,
                     simple_true=True,
                     simple_false=False,
                     complex_1={'level0': 'value'},
                     complex_2={'string': 'value', 'level0': {'level1': 'value'}})


def test_formatting(sample_operation):
    """Verify that JSON formatting works."""
    sample_operation.id = 'id'
    operation_json = sample_operation.to_full_json()

    assert 'id' not in operation_json
    assert 'creationTime' not in operation_json

    assert operation_json['deviceId'] == sample_operation.device_id
    assert operation_json['description'] == sample_operation.description
    assert operation_json['status'] == sample_operation.status

    assert operation_json['simple_string'] == sample_operation.simple_string
    assert operation_json['simple_int'] == sample_operation.simple_int
    assert operation_json['simple_float'] == sample_operation.simple_float
    assert operation_json['simple_true'] is True
    assert operation_json['simple_false'] is False
    assert operation_json['complex_1']['level0'] == 'value'
    assert operation_json['complex_2']['level0']['level1'] == 'value'

    expected_keys = {'deviceId', 'status', 'description',
                     'simple_string', 'simple_int', 'simple_float', 'simple_true', 'simple_false',
                     'complex_1', 'complex_2'}
    assert set(operation_json.keys()) == expected_keys
