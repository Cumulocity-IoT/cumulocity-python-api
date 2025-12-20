import base64
import logging
import ssl
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

import aiohttp
import certifi
import orjson


ACCEPT_MANAGED_OBJECT = 'application/vnd.com.nsn.cumulocity.managedobject+json'
ACCEPT_USER = 'application/vnd.com.nsn.cumulocity.user+json'
ACCEPT_CURRENT_USER = 'application/vnd.com.nsn.cumulocity.currentuser+json'
ACCEPT_GLOBAL_ROLE = 'application/vnd.com.nsn.cumulocity.group+json'
CONTENT_AUDIT_RECORD = 'application/vnd.com.nsn.cumulocity.auditrecord+json'
CONTENT_MANAGED_OBJECT = 'application/vnd.com.nsn.cumulocity.managedobject+json'
CONTENT_MEASUREMENT_COLLECTION = 'application/vnd.com.nsn.cumulocity.measurementcollection+json'

logger = logging.getLogger(__name__)


def loggable_params(params):
    """Provide a log-friendly formatted string of HTTP parameters."""
    if not params:
        return '-'
    return ', '.join(f"{k}={v}" for k, v in params.items())


class ProcessingMode(StrEnum):
    """Cumulocity REST API processing modes."""
    PERSISTENT = 'PERSISTENT'
    TRANSIENT = 'TRANSIENT'
    QUIESCENT = 'QUIESCENT'


class HttpMethod(StrEnum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    OPTIONS = "OPTIONS"
    HEAD = "HEAD"


class HttpError(Exception):
    """Base class for technical HTTP errors."""
    def __init__(self, method: str, url: str, code: int, message: str):
        self.method = method
        self.url = url
        self.code = code
        self. message = message

    def __repr__(self):
        return f'HTTP {self.code}: {self.method} {self.url} - {self.message}'


class UnauthorizedError(HttpError):
    """Error raised for unauthorized access."""
    def __init__(self, method: str, url: str = None, message: str = "Unauthorized."):
        super().__init__(method, url, 401, message)


class MissingTfaError(UnauthorizedError):
    """Error raised for unauthorized access."""
    def __init__(self, method: str, url: str = None, message: str = "Missing TFA Token."):
        super().__init__(method, url, message)


class AccessDeniedError(HttpError):
    """Error raised for denied access."""
    def __init__(self, method: str, url: str = None, message: str = "Access denied."):
        super().__init__(method, url, 403, message)


class Auth(Protocol):
    """Protocol class for auth providers."""

    def get_username(self):
        """Read username."""
        ...

    def build_auth_header(self) -> str:
        """Build an HTTP auth header."""   # TODO: check if this documentation is visible in docs and code hints
        ...


@dataclass(frozen=True, slots=True)
class BasicAuth:
    """Basic auth provider."""
    username: str
    password: str

    def get_username(self):
        return self.username

    def build_auth_header(self) -> str:
        token = f"{self.username}:{self.password}"
        return f"Basic {base64.b64encode(token.encode()).decode()}"


@dataclass(frozen=True, slots=True)
class BearerAuth:
    """Bearer auth provider."""
    token: str

    def get_username(self):
        return self.token  # TODO: read from JWT token

    def build_auth_header(self) -> str:
        return f"Bearer {self.token}"


class CumulocityRestApi(object):

    def __init__(
            self,
            base_url: str,
            tenant_id: str,
            auth: Auth,
            application_key: str = None,
            processing_mode: str = None
    ):
        self.base_url = base_url.rstrip('/') + '/'
        self.tenant_id = tenant_id
        self.auth = auth
        self.application_key = application_key
        self.processing_mode = processing_mode
        self._session = None

    async def __aenter__(self) -> 'CumulocityRestApi':
        _ = self.session  # ensure session is created
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._session and not self._session.closed:
            await self._session.close()

    @property
    def session(self) -> aiohttp.ClientSession:
        if not self._session:
            # ensure certifi-based SSL verification
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            # initialize default session parameters
            headers = {
                "Authorization": self.auth.build_auth_header(),
                "Accept": "application/json",  # default, can be overridden per request
            }
            if self.application_key:
                headers["X-Cumulocity-Application-Key"] = self.application_key
            if self.processing_mode:
                headers["X-Cumulocity-Processing-Mode"] = self.processing_mode
            # create session
            self._session = aiohttp.ClientSession(
                connector=aiohttp.TCPConnector(ssl=ssl_context),
                base_url=self.base_url,
                headers=headers,
            )
            return self._session
        return self._session

    @property
    def  username(self):
        return None  # todo resolve from auth


    async def request(
            self,
            method: str,
            resource: str,
            params: dict = None,
            json: dict = None,
            accept: str = None,  # application/json is assumed/automatically inserted
            content_type: str = None,  # application/json is assumed/automatically inserted if there is content
    ) -> dict:
        """Perform an HTTP request.

        Args:
            method(str): The HTTP method to use.
            resource(str): The resource path.
            params (dict): Additional request parameters
            json (dict): JSON body (nested dict)
            accept(str): Accept header value; `application/json` is assumed/automatically inserted if None
            content_type(str): Content-Type header value; `application/json` is assumed/automatically inserted
                if `json` is provided.

        Returns:
            The JSON response (nested dict), {} if no response body is returned.

        Raises:
            KeyError:  if the resources is not found (404)
            ValueError:  if the request cannot be processes (5xx) or cannot be processed for other reasons
                (only 2xx is accepted).
        """
        if json is not None:
            content_type = content_type or "application/json"
        async with self.session.request(
                method=method,
                url=resource,
                params=params,
                data=orjson.dumps(json) if json else None,
                headers={k: v for k, v in {"Accept": accept, "Content-Type": content_type}.items() if v}
        ) as r:
            logger.debug(
                "%s %s %s %s %s",
                method,
                r.status,
                resource,
                "-" if not params else ", ".join(f"{k}={v}" for k, v in params.items()),
                "-" if not json else orjson.dumps(json),
            )
            if r.status == 401:
                raise UnauthorizedError(method, resource, message=(await r.json())['message'])
            if r.status == 403:
                raise AccessDeniedError(method, resource, message=(await r.json())['message'])
            if r.status == 404:
                raise KeyError(f"No such object: {resource}")
            if 500 <= r.status <= 599:
                raise ValueError(f"Invalid {method} request. Status: {r.status}, Response:\n {await r.text()}")
            if r.status not in (200, 201, 204):
                raise ValueError(f"Unable to perform {method} request. Status: {r.status}, Response:\n {await r.text()}")
            if r.status in (200, 201):
                return orjson.loads(await r.read())
            return {}

    async def get(self, resource: str, params: dict = None, accept: str = None) -> dict:
        return await self.request("GET", resource, params, None, accept=accept)

    async def post(self, resource: str, json: dict, accept: str = None, content_type: str = None) -> dict:
        return await self.request("POST", resource, None, json, accept=accept, content_type=content_type)

    async def put(self, resource: str, json: dict, params: dict = None, accept: str = None, content_type: str = None) -> dict:
        return await self.request("PUT", resource, params, json, accept=accept, content_type=content_type)

    async def delete(self, resource: str, params: dict = None) -> dict:
        return await self.request("DELETE", resource, params)
