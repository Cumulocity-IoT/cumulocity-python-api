# Copyright (c) 2025 Cumulocity GmbH

from __future__ import annotations

import base64
import json
import time


class JWT:
    """Simple JWT toolkit.

    This class is used to parse Cumulocity's JWT tokens.
    """

    def __init__(self, token: str | bytes):
        self.token = token if isinstance(token, bytes) else token.encode('utf-8')
        self._body: dict | None = None

    @property
    def payload(self):
        """Return the JWT payload as JSON document."""
        if not self._body:
            jwt_parts = self.token.split(b'.')
            if len(jwt_parts) != 3:
                raise ValueError("Unexpected token format (Invalid number of parts, not an JWT?).")
            # The JWT body might not be padded, hence we add padding
            # characters which are ignored if they are not necessary.
            # See: https://gist.github.com/perrygeo/ee7c65bb1541ff6ac770,
            # https://stackoverflow.com/questions/2941995
            body = jwt_parts[1] + b'=='
            self._body = json.loads(base64.b64decode(body))
        return self._body

    @property
    def username(self):
        """Read the username from the token payload."""
        return self.get_claim('sub')

    @property
    def tenant_id(self):
        """Read the tenant ID from the token payload."""
        return self.get_claim('ten')

    def get_claim(self, claim: str):
        """Read a claim from the token payload."""
        return self.payload[claim]

    def get_valid_seconds(self):
        """Return the number of seconds the token before the tokens expires.

        Returns:
            The number of seconds the token remains valid.
        """
        return self.payload['exp'] - time.time()

    def is_valid(self, min_seconds: int = None):
        """Check whether the token is valid.

        Args:
            min_seconds: Minimum number of seconds of validity.
        """
        if not min_seconds:
            min_seconds = 0
        return time.time() + min_seconds > int(self.payload['exp'])
