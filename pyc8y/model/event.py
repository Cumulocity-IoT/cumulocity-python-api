from datetime import datetime
from typing import BinaryIO

from pyc8y.base import CumulocityRestApi
from pyc8y.model.base import CumulocityObject, json_property, datetime_property, id_property
from pyc8y.model.util import assert_c8y, assert_id


class Event(CumulocityObject):
    """Represent an instance of an event object in Cumulocity.

    Instances of this class are returned by functions of the corresponding
    Events API. Use this class to create new or update Event objects.

    See also: https://cumulocity.com/api/#tag/Events
    """

    _c8y_resource = "/event/events"
    _c8y_accept = "application/vnd.com.nsn.cumulocity.event+json"
    _c8y_content = "application/vnd.com.nsn.cumulocity.event+json"

    def __init__(
            self,
            c8y: CumulocityRestApi = None,
            type: str = None,   # noqa (type)
            time: str | datetime = None,
            source: str = None,
            text: str = None,
            **kwargs
    ):
        super().__init__(c8y, **kwargs)
        self.type = type
        self.source = source
        self.time = ensure_timestring(time)
        self.text = text

    type = json_property("type")
    source = id_property("source")
    text = json_property("text")
    time = json_property("time")
    datetime = datetime_property("datetime")
    creation_time = json_property("creationTime")
    creation_datetime = datetime_property("creationTime")
    update_time = json_property("lastUpdated")
    update_datetime = datetime_property("lastUpdated")
    last_updated = json_property("lastUpdated")
    last_updated_datetime = json_property("lastUpdated")

    @property
    def attachment_path(self) -> str:
        return f"{self.resource_path}/binaries"

    def has_attachment(self) -> bool:
        """Check whether the event has a binary attachment.

        Event objects that have an attachment feature a `c8y_IsBinary`
        fragment. This function checks the presence of that fragment.

        Note: This does not query the database. Hence, the information might
        be outdated if a binary was attached _after_ the event object was
        last read from the database.

        Returns:
            True if the event object has an attachment, False otherwise.
        """
        return "c8y_IsBinary" in self._json

    async def create_attachment(self, file: str | BinaryIO, content_type: str = None) -> dict:
        """Create the binary attachment.

        Args:
            file (str|BinaryIO): File-like object or a file path
            content_type (str):  Content type of the file sent
                (default is application/octet-stream)

        Returns:
            Attachment details as JSON object (dict).
        """
        assert_c8y(self)
        assert_id(self)
        return await self.c8y.post_file(self.attachment_path, file=file, content_type=content_type)

    async def update_attachment(self, file: str | BinaryIO, content_type: str = None) -> dict:
        """Update the binary attachment.

        Args:
            file (str|BinaryIO): File-like object or a file path
            content_type (str):  Content type of the file sent
                (default is application/octet-stream)

        Returns:
            Attachment details as JSON object (dict).
        """
        assert_c8y(self)
        assert_id(self)
        return self.c8y.put_file(self.attachment_path, file=file)

    async def delete_attachment(self) -> None:
        """Remove the binary attachment."""
        assert_c8y(self)
        assert_id(self)
        await self.c8y.delete(self.attachment_path)
