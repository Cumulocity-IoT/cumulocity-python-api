# Copyright (c) 2025 Cumulocity GmbH

import time

from c8y_api import CumulocityApi
from c8y_api.model import BulkOperation, DeviceGroup, Operation


def test_CRU(live_c8y: CumulocityApi, sample_device):  # noqa
    """Verify that basic creation, lookup and update of Operations works as expected."""

    # (1) Create a device group for the sample device
    group:DeviceGroup = DeviceGroup(live_c8y,
                                    root=True,
                                    name=sample_device.name + '_Group').create()
    group.add_child_asset(sample_device)


    # (2) create bulk operation
    bulk:BulkOperation = BulkOperation(live_c8y,
                          group_id=group.id,
                          start_time='now',
                          creation_ramp=1,
                          operation_prototype={
                              'description': f"Update firmware for device group '{group.name}'.",
                              'c8y_FirmWare': {
                                  'version': '1.0.0'
                              }}
                          ).create()

    # wait for the bulk operation to be processed
    time.sleep(5)

    # Check if bulk operation was created
    all_ids = [x.id for x in live_c8y.bulk_operations.get_all()]
    assert bulk.id in all_ids

    # (3) initially the status should be EXECUTING/COMPLETED as all
    #     child operations should have been created but not completed
    bulk = live_c8y.bulk_operations.get(bulk.id)
    assert bulk.general_status == BulkOperation.GeneralStatus.EXECUTING
    assert bulk.status == BulkOperation.Status.COMPLETED
    assert bulk.progress.all == 1
    assert bulk.progress.pending == 1

    # (4) find child operations
    op = live_c8y.operations.get_all(bulk_id=bulk.id)[0]
    assert op.status == Operation.Status.PENDING

    # (5) kill child operations
    op.status = Operation.Status.FAILED
    op.update()

    # (5) bulk operation should now be in PENDING state
    bulk = live_c8y.bulk_operations.get(bulk.id)
    assert bulk.general_status in [BulkOperation.GeneralStatus.COMPLETED_WITH_FAILURES,
                                   BulkOperation.GeneralStatus.FAILED]
    assert bulk.progress.all == 1
    assert bulk.progress.failed == 1
    assert live_c8y.operations.get_count(bulk_id=bulk.id) == 1

    # (6) cleanup
    #     The bulk operation cannot be deleted physically
    group.delete()
