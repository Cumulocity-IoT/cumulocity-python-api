# Copyright (c) 2025 Cumulocity GmbH

import pytest

from c8y_api import CumulocityApi
from c8y_api.model.identity import ExternalId

from util.testing_util import RandomNameGenerator


def test_CRUD(live_c8y: CumulocityApi, sample_device):
    """Verify that basic creation/removal and lookup of ID works as expected."""

    id_ref1 = RandomNameGenerator.random_name(3, '-') + '-12345'
    id_ref2 = RandomNameGenerator.random_name(3, '-') + '-12345'
    id_type = 'external_id_type'

    external_id1 = ExternalId(live_c8y, id_ref1, 'external_id_type', sample_device.id)
    external_id1.create()

    external_id2 = ExternalId(live_c8y, id_ref2, 'external_id_type', sample_device.id)
    external_id2.create()

    try:
        # retrieve all linked external id
        ids = {i.external_id for i in live_c8y.identity.get_all(sample_device.id)}
        assert ids == {id_ref1, id_ref2}

        # retrieve the referenced object
        obj = external_id1.get_object()
        # -> it is identical to the sample device
        assert obj.id == sample_device.id

        # retrieve the object ID via API
        # -> identical to sample device ID
        assert live_c8y.identity.get_id(id_ref1, id_type) == sample_device.id

        # retrieve object via external id
        # -> identical to sample device ID
        assert live_c8y.identity.get_object(id_ref2, id_type).id == sample_device.id

    finally:
        external_id1.delete()
        live_c8y.identity.delete(id_ref2, id_type)

    with pytest.raises(KeyError):
        live_c8y.identity.get(id_ref1, id_type)

    with pytest.raises(KeyError):
        live_c8y.identity.get(id_ref2, id_type)
