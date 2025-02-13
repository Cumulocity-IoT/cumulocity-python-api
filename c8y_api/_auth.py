# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

import base64
from typing import Any

from requests.auth import HTTPBasicAuth, AuthBase

from c8y_api._jwt import JWT


class HTTPBearerAuth(AuthBase):
    """Token based authentication."""

    def __init__(self, token: str):
        self.token = token

    def __call__(self, r):
        r.headers['Authorization'] = 'Bearer ' + self.token
        return r


class AuthUtil:
    """Authorization utility functions."""

    @staticmethod
    def parse_auth_string(auth_string: str) -> AuthBase:
        """Parse a given auth string into a corresponding auth object.

        Args:
            auth_string (str):  Complete Auth string (including the type prefix
                like BASIC etc.) as it comes with an Authorization HTTP header

        Returns:
            An AuthBase instance for this auth string.
        """
        return AuthUtil._parse_with(auth_string,
                                    basic_fun=AuthUtil.parse_basic_auth_value,
                                    bearer_fun=AuthUtil.parse_bearer_auth_value)

    @staticmethod
    def get_tenant_id(auth: AuthBase) -> str:
        """Read the tenant ID from authorization information.

        Args:
            auth (AuthBase):  Auth instance, only HTTPBasicAuth and
                HTTPBearerAuth are supported

        Returns:
            The tenant ID encoded in the auth information.

        Raises:
            ValueError:  if the tenant ID cannot be resolved or an unsupported
                AuthBase instance was provided.
        """
        def resolve_basic(a):
            username = a.username
            if '/' not in username:
                raise ValueError(f"Unable to isolate tenant ID from username: {username}")
            return username[:username.index('/')]

        def resolve_bearer(a):
            try:
                tenant_id = JWT(a.token).tenant_id
            except KeyError:
                tenant_id = None
            if not tenant_id:
                raise ValueError("Unable to resolve tenant ID. JWT does not appear to include it.")
            return tenant_id

        return AuthUtil._parse_auth_with(auth, resolve_basic, resolve_bearer)

    @staticmethod
    def get_username(auth: AuthBase) -> str:
        """Read the username from authorization information.

        Args:
            auth (AuthBase):  Auth instance, only HTTPBasicAuth and
                HTTPBearerAuth are supported

        Returns:
            The username encoded in the auth information.

        Raises:
            ValueError:  if the username cannot be resolved or an unsupported
                AuthBase instance was provided.
        """
        def resolve_basic(a):
            return a.username

        def resolve_bearer(a):
            return JWT(a.token).username

        return AuthUtil._parse_auth_with(auth, resolve_basic, resolve_bearer)

    @staticmethod
    def parse_basic_auth_value(auth_value: str) -> HTTPBasicAuth:
        """Parse a BASIC HTTP auth string.

        Args:
             auth_value:  The Authorization header value (Base64 encoded,
                without the 'BASIC' type prefix)

        Returns:
            An HTTPBasicAuth object.
        """
        decoded = base64.b64decode(bytes(auth_value, 'utf-8'))
        parts = [x.decode('utf-8') for x in decoded.split(b':', 1)]
        return HTTPBasicAuth(username=parts[0], password=parts[1])

    @staticmethod
    def parse_bearer_auth_value(auth_value: str) -> HTTPBearerAuth:
        """Parse a BEARER HTTP auth string.

        Args:
             auth_value:  The Authorization header value (Base64 encoded,
                without the 'BEARER' type prefix)

        Returns:
            An HTTPBearerAuth object.
        """
        return HTTPBearerAuth(token=auth_value)

    @staticmethod
    def _parse_auth_with(auth: AuthBase, basic_fun, bearer_fun) -> Any:
        """Parse an auth instance.

        Args:
            auth (AuthBase): Authentication information
            basic_fun: Parsing function to be applied for BASIC auth
            bearer_fun: Parsing function to be applied for BEARER auth

        Returns:
            Whatever is returned by the parsing functions.

        Raises:
            ValueError:  if the auth string is of an unsupported type.
        """
        if isinstance(auth, HTTPBasicAuth):
            return basic_fun(auth)
        if isinstance(auth, HTTPBearerAuth):
            return bearer_fun(auth)
        raise ValueError(f"Unable to parse authentication information! Unexpected AuthBase instance: {auth.__class__}")

    @staticmethod
    def _parse_with(auth_string: str, basic_fun, bearer_fun) -> Any:
        """Parse an auth header string.

        Args:
            auth_string (str): Complete auth string (including type prefix).
            basic_fun: Parsing function to be applied for BASIC auth
            bearer_fun: Parsing function to be applied for BEARER auth

        Returns:
            Whatever is returned by the parsing functions.

        Raises:
            ValueError:  if the auth string is of an unsupported type.
        """
        auth_type, auth_value = auth_string.split(' ')

        if auth_type.upper() == 'BASIC':
            return basic_fun(auth_value)
        if auth_type.upper() == 'BEARER':
            return bearer_fun(auth_value)

        raise ValueError(f"Unexpected authorization header type: {auth_type}")
