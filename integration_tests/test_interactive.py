# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from c8y_tk.interactive import CumulocityContext


def test_context(test_environment):
    """Verify that the CumulocityContext class instantiates as expected."""

    c8y = CumulocityContext()
    assert c8y.users.get_current().username

    with CumulocityContext() as c8y2:
        assert c8y2.users.get_current().username
