import pytest

from pyc8y.base import CumulocityRestApi, BasicAuth


@pytest.mark.parametrize("method, json, accept, content_type", [
    ("GET", None, None, None),
    ("GET", None, "text/plain", None),
    ("POST", {"a":1}, None, None),
    ("POST", {"a":1}, "text/plain", "text/plain"),
    ("PUT", {"a":1}, None, None),
    ("PUT", {"a":1}, "text/plain", "text/plain"),
])
@pytest.mark.asyncio
async def test_session_headers(method, json, accept, content_type):
    """Ensure that session headers are merged with request headers."""

    async with (CumulocityRestApi("https://httpbin.org", tenant_id="", auth=BasicAuth("user", "auth")) as c8y):
        result = await c8y.request(method, "/anything", accept=accept, content_type=content_type)
        # -> there is always an Authorization header
        assert "Authorization" in result["headers"]
        # -> there is always an Accept header
        assert result["headers"]["Accept"] == accept or "application/json"
        # -> body defines whether there is a Content-Type header
        if json:
            assert result["headers"]["Content-Type"] == content_type or "application/json"
        else:
            assert "Content-Type" not in result["headers"]
