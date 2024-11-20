# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.
# pylint: disable=protected-access

import getpass
import os
import time
from urllib.parse import urlparse

from c8y_api import CumulocityApi, UnauthorizedError, MissingTfaError, HTTPBearerAuth, CumulocityRestApi, HttpError
from c8y_api._jwt import JWT


class CumulocityContext(CumulocityApi):
    """Context manager to be used for interactive sessions.

    The context manager ensures that a valid Cumulocity connection is
    available at runtime.  It uses standard environment variables when
    defined (C8Y_BASEURL, C8Y_TENANT, C8Y_USER, C8Y_PASSWORD, as well
    as C8Y_TOKEN) and interactively requests updated information in case
    some data is missing.

    ```
    with CumulocityContext() as c8y:
        alarms = c8y.alarms.get_all(type='cx_MyAlarm'))
        ...
    ```
    """

    _cached_passwords: dict[str, str] = {}

    @staticmethod
    def _read_variable(env_name: str, prompt: str = None, secret: bool = False):
        if env_name in os.environ:
            return os.environ[env_name]

        if not prompt:
            return None

        if secret:
            return getpass.getpass(prompt)
        return input(prompt)

    def __init__(self):

        base_url = None
        tenant_id = None
        username = None

        # (1) check if there is a token defined
        token = os.environ.get('C8Y_TOKEN', None)
        if token:
            jwt = JWT(token)
            # preserve info
            base_url = jwt.get_claim('aud')
            tenant_id = jwt.get_claim('ten')
            username = jwt.get_claim('sub')
            # check validity
            exp = int(jwt.get_claim('exp'))
            if time.time() > (exp - 60*60):
                print("Access token found, but invalidated as it was almost expired.")
                token = None

        # (2) no token (or invalidated)
        if not token:
            # read necessary info for auth, this can also be resolved from an invalid token
            base_url = base_url or self._read_variable(
                'C8Y_BASEURL',
                "Please enter the Cumulocity base URL or hostname:"
            )

            tenant_id = tenant_id or self._read_variable(
                'C8Y_TENANT',
                "Please enter the Cumulocity tenant ID:"
            )
            username = username or self._read_variable(
                'C8Y_USER',
                "Please enter the Cumulocity username:"
            )
            if not urlparse(base_url).scheme:
                base_url = f'https://{base_url}'

            # authenticate (in a loop in case of wrong passwords entered)
            needs_tfa = False
            while not token:
                # read password (might already been cached)
                password = self._cached_passwords.get(username, None)
                password = password or self._read_variable(
                    'C8Y_PASSWORD',
                    "Please enter the Cumulocity password:",
                    secret=True
                )
                # if no password is provided, exit the loop
                if not password:
                    raise UnauthorizedError("No password provided. Authentication failed.")
                # preserve password for next time
                self._cached_passwords[username] = password
                # request TFA code if needed
                tfa_code = input("Please enter a current TFA code:") if needs_tfa else None

                try:
                    token, _ = CumulocityRestApi.authenticate(
                        base_url=base_url,
                        tenant_id=tenant_id,
                        username=username,
                        password=password,
                        tfa_token=tfa_code,
                    )
                except MissingTfaError:
                    needs_tfa = True
                    # we can just go to the next loop iteration, password is cached
                    continue
                except HttpError:
                    print(f"Invalid username or password (URL: {base_url}, User: {username}).")
                    self._cached_passwords.pop(username, None)
                    continue

        # (1) build new connection from token and put to cache
        os.environ['C8Y_TOKEN'] = token
        super().__init__(base_url=base_url, tenant_id=tenant_id, auth=HTTPBearerAuth(token))

    def __enter__(self) -> CumulocityApi:
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return True
