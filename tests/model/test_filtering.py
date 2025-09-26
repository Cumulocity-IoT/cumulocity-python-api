# Copyright (c) 2025 Cumulocity GmbH

from unittest.mock import Mock

import pytest

from c8y_api import CumulocityApi
from c8y_api.model import AuditRecords, Events, Alarms, Operations, Inventory, DeviceGroupInventory, DeviceInventory, \
    Users, GlobalRoles


@pytest.mark.parametrize('resource_class', [
    Events,
    Alarms,
    Operations,
    AuditRecords,
    Inventory,
    DeviceInventory,
    DeviceGroupInventory,
    Users,
    GlobalRoles,  # additional test in test_global_role.py
])
def test_client_side_filtering(resource_class):
    """Verify that client side filtering works as expected.

    This test prepares a mocked CumulocityApi and runs the get_all function
    against it. The REST GET is mocked as well as corresponding matcher
    results. The test verifies that the matcher is invoked and applied.
    """
    # create mock CumulocityApi instance
    c8y = CumulocityApi(base_url='some.host.com', tenant_id='t123', username='user', password='pass')
    resource = resource_class(c8y=c8y)

    # prepare mock data and corresponding matchers
    # the get function is invoked until there are no results (empty list), the results
    # are stored in an array by object name
    get_data = [
        {resource.object_name: x, 'statistics': {'totalPages': 1}} for x in
        [
            # need to prepare data for all kind of formats ...
            [{'id': 1, 'source':{'id': 1}}, {'id': 2, 'source':{'id': 2}}, {'id': 3, 'source':{'id': 3}}],
            []
        ]
    ]
    include_results = [True, False, True]
    exclude_results = [True, False]

    c8y.get = Mock(side_effect=get_data)
    include_matcher = Mock(safe_matches=Mock(side_effect=include_results))
    exclude_matcher = Mock(safe_matches=Mock(side_effect=exclude_results))

    # run get_all/select
    result = resource.get_all(include=include_matcher, exclude=exclude_matcher)

    # -> result should only contain filtered documents
    #    1,2,3 -> 1,3 -> 3
    assert ['3'] == [str(x.id) for x in result]
    # -> include matcher should have been called for each document
    assert include_matcher.safe_matches.call_count == len(include_results)
    # -> exclude matcher should have been called for each included
    assert exclude_matcher.safe_matches.call_count == len(exclude_results)
