# Copyright (c) 2025 Cumulocity GmbH

from datetime import timedelta

from c8y_api import CumulocityApi
from c8y_api.model import AuditRecord
from c8y_api.model._util import _DateUtil  # noqa

from util.testing_util import RandomNameGenerator


def test_CR(live_c8y: CumulocityApi, sample_device):  # noqa (case)
    """Verify that basic creation, lookup and update of Audit Records
     works as expected."""

    name = RandomNameGenerator.random_name()

    # (1) create audit record
    before = _DateUtil.now()
    record = AuditRecord(live_c8y, type=f'{name}_type', source=sample_device.id, time='now',
                         severity=AuditRecord.Severity.INFORMATION,
                         activity=f'{name} activity', text=f'detailed {name} text',
                         application=f'{name}_app', user=live_c8y.username).create()
    after = _DateUtil.now()

    # -> there should be exactly one audit record with that source
    records = live_c8y.audit_records.get_all(source=sample_device.id)
    assert len(records) == 1
    assert records[0].id == record.id

    # -> there should be exactly one audit record with that application/user
    records = live_c8y.audit_records.get_all(application=record.application,
                                             user=record.user)
    assert len(records) == 1
    assert records[0].id == record.id

    # -> there should be at least one audit record within that timeframe
    records = live_c8y.audit_records.get_all(before=after, after=before)
    assert len(records) >= 1
    assert record.id in [r.id for r in records]

    # -> there should be at least one audit record within the last 5 seconds
    records = live_c8y.audit_records.get_all(min_age=timedelta(microseconds=0.1),
                                             max_age=timedelta(seconds=5.0))
    assert len(records) >= 1
    assert record.id in [r.id for r in records]
