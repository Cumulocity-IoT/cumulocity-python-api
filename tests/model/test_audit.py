# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=protected-access

import json
import os
from typing import List

import pytest

from c8y_api.model import AuditRecord, Change
from c8y_api.model._util import _DateUtil
from util.testing_util import RandomNameGenerator


def fix_sample_jsons() -> List[dict]:
    """Read sample jsons from file. This is not a pytest fixture."""
    path = os.path.dirname(__file__) + '/audit_records.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        records = json.load(f)
        return records['auditRecords']


@pytest.mark.parametrize('sample_json', fix_sample_jsons())
def test_parsing(sample_json):
    """Verify that parsing an Audit Record from JSON works."""
    record = AuditRecord.from_json(sample_json)

    assert record.type == sample_json['type']
    if 'source' in sample_json:
        assert record.source == sample_json['source']['id']
    assert record.activity == sample_json['activity']
    assert record.text == sample_json['text']
    if 'severity' in sample_json:
        assert record.severity == sample_json['severity']
    assert record.user == sample_json['user']
    if 'application' in sample_json:
        assert record.application == sample_json['application']
    if 'changes' in sample_json:
        assert len(record.changes) == len(sample_json['changes'])
        for i, c in enumerate(record.changes):
            assert c.attribute == sample_json['changes'][i]['attribute']
            assert c.type == sample_json['changes'][i]['type']
            assert c.new_value == sample_json['changes'][i]['newValue']
            assert c.previous_value == sample_json['changes'][i]['previousValue']

    assert record.time == sample_json['time']
    assert record.creation_time == sample_json['creationTime']
    assert record.datetime == _DateUtil.to_datetime(sample_json['time'])

    if record.type == 'Alarm':
        assert record.com_cumulocity_model_event_AuditSourceDevice.id == '18924'

def test_formatting():
    """Verify that JSON formatting works."""

    record = AuditRecord(type=RandomNameGenerator.random_name(), time='now', source='source-id',
                         activity='audit activity', text='audit text',
                         severity=AuditRecord.Severity.INFORMATION,
                         application='some application',
                         user='some@cumulocity.com',
                         changes=[
                             Change(attribute='attr', new_value='new', previous_value='old', type='type'),
                             Change(attribute='attr2', new_value='new2', previous_value='old2', type='type2')
                         ],
                         customFragment={'value': 12},
                         property=42)
    record.id = 'id'
    record.creation_time = '2023-03-23T22:33:44.555Z'
    record_json = record.to_full_json()

    assert 'id' not in record_json

    assert record_json['type'] == record.type
    assert record_json['source']['id'] == record.source
    assert record_json['severity'] == record.severity
    assert record_json['activity'] == record.activity
    assert record_json['text'] == record.text

    assert record_json['application'] == record.application
    assert record_json['user'] == record.user

    assert record_json['time'] == record.time
    assert 'creationTime' not in record_json

    assert len(record_json['changes']) == 2
    assert record_json['changes'][1]['attribute'] == 'attr2'
    assert record_json['changes'][1]['type'] == 'type2'
    assert record_json['changes'][1]['newValue'] == 'new2'
    assert record_json['changes'][1]['previousValue'] == 'old2'

    expected_keys = {'type', 'time', 'source', 'severity',
                     'activity', 'text', 'application', 'user',
                     'changes', 'customFragment', 'property'}
    assert set(record_json.keys()) == expected_keys


def test_no_changes():
    """Verify that the changes fragment is only present if provided."""
    record = AuditRecord(type=RandomNameGenerator.random_name())
    record_json = record.to_full_json()
    assert 'changes' not in record_json


def test_empty_changes():
    """Verify that the changes fragment can be present but empty."""
    record = AuditRecord(type=RandomNameGenerator.random_name(), changes=[])
    record_json = record.to_full_json()
    assert 'changes' in record_json
    assert len(record_json['changes']) == 0
