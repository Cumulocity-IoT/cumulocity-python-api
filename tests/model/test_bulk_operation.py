# Copyright (c) 2025 Cumulocity GmbH

import json
import os
from datetime import datetime, timezone
from unittest.mock import Mock

from dateutil import parser
from typing import List

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import BulkOperation, BulkOperations


def fix_sample_jsons() -> List[dict]:
    """Read sample jsons from file. This is not a pytest fixture."""
    path = os.path.dirname(__file__) + '/bulk_operations.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        root = json.load(f)
        return root['bulkOperations']


def test_parsing_collection():
    """Verify that parsing a collection works as expected.
    The bulk operations JSON is non-standard as the name differs from the REST endpoint."""
    path = os.path.dirname(__file__) + '/bulk_operations.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        document = json.load(f)
    page1 = document
    page2 = {"bulkOperations": []}

    mock_c8y = CumulocityApi("base_url", "tenant_id", "user", "password")
    mock_c8y.get = Mock(side_effect=[page1, page2])
    operations = BulkOperations(mock_c8y).get_all()
    assert len(operations) == len(document['bulkOperations'])
    assert operations[0].id == document['bulkOperations'][0]['id']


@pytest.mark.parametrize('sample_json', fix_sample_jsons())
def test_parsing(sample_json):
    """Verify that parsing a Bulk Operation from JSON works."""
    operation = BulkOperation.from_json(sample_json)

    assert operation.id == sample_json['id']
    if 'groupId' in sample_json:
        assert operation.group_id == sample_json['groupId']
    if 'failedParentId' in sample_json:
        assert operation.failed_parent_id == sample_json['failedParentId']

    assert operation.creation_ramp == sample_json['creationRamp']
    assert operation.status == sample_json['status']
    assert operation.general_status == sample_json['generalStatus']

    assert isinstance(operation.start_datetime, datetime)
    assert operation.start_time == sample_json['startDate']

    assert operation.progress.all == sample_json['progress']['all']
    assert operation.progress.successful == sample_json['progress']['successful']

    assert operation.operation_prototype.description == sample_json['operationPrototype']['description']


def test_formatting():
    """Verify that simple formatting as JSON works as expected."""
    op = BulkOperation(
        group_id='group_id',
        failed_parent_id='failed-parent-id',
        start_time='now',
        creation_ramp=123,
        operation_prototype={
            'description': 'some description',
            "c8y_Firmware": {
                "name": "MyFirmware",
                "url": "http://test:test@example.com",
                "version": "1.0.0"
                }
            },
        note='custom note')

    op.id = 'some-id'
    op_json = op.to_full_json()

    assert 'id' not in op_json
    assert op_json['groupId'] == op.group_id
    assert op_json['failedParentId'] == op.failed_parent_id
    # verify that start date is quite recent
    assert (datetime.now(timezone.utc) - parser.parse(op.start_time)).total_seconds() < 1.0

    assert op_json['creationRamp'] == op.creation_ramp

    assert op_json['operationPrototype']['description'] == op.operation_prototype.description
    assert op_json['operationPrototype']['c8y_Firmware']['url'] == op.operation_prototype.c8y_Firmware.url

    assert op_json['note'] == op.note
