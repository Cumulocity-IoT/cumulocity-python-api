# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

import base64
from typing import List, Set, Any
from unittest.mock import Mock

import jwt
import pytest

from c8y_api.model._base import CumulocityObject

from util.testing_util import RandomNameGenerator


def get_ids(objs: List[CumulocityObject]) -> Set[str]:
    """Isolate the ID from a list of database objects."""
    return {o.id for o in objs}


def isolate_last_call_arg(mock: Mock, name: str, pos: int = None) -> Any:
    """Isolate arguments of the last call to a mock.

    The argument can be specified by name and by position.

    Args:
        mock (Mock): the Mock to inspect
        name (str): Name of the parameter
        pos (int): Position of the parameter

    Returns:
        Value of the call argument

    Raises:
        KeyError:  if the argument was not given/found by name and the
            position was not given/out of bounds.
    """
    mock.assert_called()
    args, kwargs = mock.call_args
    if name in kwargs:
        return kwargs[name]
    if len(args) > pos:
        return args[pos]
    raise KeyError(f"Argument not found: '{name}'. "
                   f"Not given explicitly and position ({pos}) out of of bounds.")


def isolate_all_call_args(mock: Mock, name: str, pos: int = None) -> List[Any]:
    """Isolate arguments of all calls to a mock.

    The argument can be specified by name and by position.

    Args:
        mock (Mock): the Mock to inspect
        name (str): Name of the parameter
        pos (int): Position of the parameter

    Returns:
        List of value of the call argument

    Raises:
        KeyError:  if the argument was not given/found by name and the
            position was not given/out of bounds.
    """
    mock.assert_called()
    result = []
    for args, kwargs in mock.call_args_list:
        if name in kwargs:
            result.append(kwargs[name])
        elif len(args) > pos:
            result.append(args[pos])
    if not result:
        raise KeyError(f"Argument not found in any of the calls: '{name}', pos: {pos}.")
    return result


@pytest.fixture(scope='function')
def random_name() -> str:
    """Provide a random name."""
    return RandomNameGenerator.random_name()


def b64encode(auth_string: str) -> str:
    """Encode a string with base64. This uses UTF-8 encoding."""
    return base64.b64encode(auth_string.encode('utf-8')).decode('utf-8')


def build_auth_string(auth_value: str) -> str:
    """Build a complete auth string from a base64 encoded auth value.
    This detects the type based on the `auth_value` contents, assuming
    that JWT tokens always start with an '{'."""
    auth_type = 'BEARER' if auth_value.startswith('ey') else 'BASIC'
    return f'{auth_type} {auth_value}'


def sample_jwt(**kwargs) -> str:
    """Create a test JWT token (as string). Additional claims ca be
    specified via `kwargs`."""
    payload = {
        'jti': None,
        'iss': 't12345.cumulocity.com',
        'aud': 't12345.cumulocity.com',
        'tci': '0722ff7b-684f-4177-9614-3b7949b0b5c9',
        'iat': 1638281885,
        'nbf': 1638281885,
        'exp': 1639491485,
        'tfa': False,
        'xsrfToken': 'something'}
    payload.update(**kwargs)
    return jwt.encode(payload, key='key')
