# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

import os
from typing import Set


def c8y_keys() -> Set[str]:
    """Provide the names of defined Cumulocity environment variables.

    Returns: A set of environment variable names, starting with 'C8Y_'
    """
    return set(filter(lambda x: 'C8Y_' in x, os.environ.keys()))


def validate_base_url(url) -> str:
    """Ensure that a given url is a proper Cumulocity base url.

    Args:
        url (str):  A URL string with or without scheme, port, path

    Returns:
        A URL string with scheme (default is https) and port but
        without trailing slash or even path.
    """
    i = url.find('://')  # scheme separator
    j = url.find('/', i + 3)  # path separator
    return ('https://' if i == -1 else '') + (url if j == -1 else url[:j])
