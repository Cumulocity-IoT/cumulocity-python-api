# Copyright (c) 2025 Cumulocity GmbH

from c8y_tk.interactive import CumulocityContext


def test_context(test_environment):
    """Verify that the CumulocityContext class instantiates as expected."""

    c8y = CumulocityContext()
    assert c8y.users.get_current().username

    with CumulocityContext() as c8y2:
        assert c8y2.users.get_current().username
