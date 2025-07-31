# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

import base64
from typing import List, Set, Any
from unittest.mock import Mock

import jwt
import pytest

from c8y_api.model._base import CumulocityObject

from util.testing_util import RandomNameGenerator


def assert_in_any(string, *strings):
    """Assert that a string is in any of a given list of strings."""
    assert any(string in s for s in strings), f"'{string}' is not in any of {strings}"


def assert_not_in_any(string, *strings):
    """Assert that a string is not in any of a given list of strings."""
    assert all(string not in s for s in strings), f"'{string}' is in one of {strings}"


def assert_all_in_any(multiple_strings, *strings):
    """Assert that multiple strings are in a given list of strings."""
    for string in multiple_strings:
        assert any(string in s for s in strings), f"'{string}' is not in any of {strings}"


def assert_all_not_in_any(multiple_strings, *strings):
    """Assert that multiple strings are in a given list of strings."""
    for string in multiple_strings:
        assert all(string not in s for s in strings), f"'{string}' is in one of {strings}"


def get_ids(objs: List[CumulocityObject]) -> Set[str]:
    """Isolate the ID from a list of database objects."""
    return {o.id for o in objs}


def isolate_last_call_arg(mock: Mock, *args: str | int, name: str = None, pos: int = None) -> Any:
    """Isolate arguments of the last call to a mock.

    The argument can be specified by name and by position, in any order or by a named parameter.

    Args:
        mock (Mock): the Mock to inspect
        args (str|int): the argument to isolate (by name, position or both)
        name (str): Name of the parameter
        pos (int): Position of the parameter

    Returns:
        Value of the call argument

    Raises:
        KeyError:  if the argument was not given/found by name and the
            position was not given/out of bounds.
    """
    mock.assert_called()
    call_args, call_kwargs = mock.call_args
    name = name or None
    pos = pos if pos is not None else -1
    for arg in args:
        if isinstance(arg, int):
            pos = arg
        elif isinstance(arg, str):
            name = arg
    if name in call_kwargs:
        return call_kwargs[name]
    if len(call_args) > pos:
        return call_args[pos]
    raise KeyError(f"Argument not found: '{name}'. "
                   f"Not given explicitly and position ({pos}) out of of bounds.")


def isolate_all_call_args(mock: Mock, *args: str | int, name: str, pos: int = None) -> List[Any]:
    """Isolate arguments of all calls to a mock.

    The argument can be specified by name and by position, in any order or by a named parameter.

    Args:
        mock (Mock): the Mock to inspect
        args (str|int): the argument to isolate (by name, position or both)
        name (str): Name of the parameter
        pos (int): Position of the parameter

    Returns:
        List of value of the call argument

    Raises:
        KeyError:  if the argument was not given/found by name and the
            position was not given/out of bounds.
    """
    mock.assert_called()
    name = name or None
    pos = pos if pos is not None else -1
    for arg in args:
        if isinstance(arg, int):
            pos = arg
        elif isinstance(arg, str):
            name = arg
    result = []
    for call_args, call_kwargs in mock.call_args_list:
        if name in call_kwargs:
            result.append(call_kwargs[name])
        elif len(call_args) > pos:
            result.append(call_args[pos])
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
