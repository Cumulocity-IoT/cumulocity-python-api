# Copyright (c) 2025 Cumulocity GmbH


from c8y_api import CumulocityApi
from c8y_api.model import Operation

from util.testing_util import RandomNameGenerator


def test_CRUD(live_c8y: CumulocityApi, sample_device):
    """Verify that basic creation, lookup and update of Operations works as expected."""

    name = RandomNameGenerator.random_name()

    # (1) create operation
    operation = Operation(live_c8y, sample_device.id, description='Description '+name,
                          c8y_Command={'text': 'Command text'})
    operation = operation.create()

    # -> operation should have been created and in PENDING state
    operations = live_c8y.operations.get_all(agent_id=sample_device.id, status=Operation.Status.PENDING)
    assert len(operations) == 1
    assert operations[0].id == operation.id

    # -> same result with get_last
    operation2 = live_c8y.operations.get_last(agent_id=sample_device.id, status=Operation.Status.PENDING)
    assert operation2.id == operation.id

    # (2) update operation
    operation.status = Operation.Status.EXECUTING
    operation.description = 'New description'
    operation.c8y_Command.text = 'Updated command text'
    operation.add_fragment('c8y_CustomCommand', value='good')
    operation.update()

    # -> all fields have been updated in Cumulocity
    operation2 = live_c8y.operations.get(operation.id)
    assert operation2.status == operation.status
    assert operation2.description == operation.description
    assert operation2.c8y_Command.text == operation.c8y_Command.text
    assert operation2.c8y_CustomCommand.value == operation.c8y_CustomCommand.value

    # (3) delete operation
    live_c8y.operations.delete_by(device_id=sample_device.id)

    # -> cannot be found anymore
    assert not live_c8y.operations.get_all(device_id=sample_device.id)
