# Copyright (c) 2025 Cumulocity GmbH

import logging
from abc import ABC, abstractmethod

from c8y_api.model._util import _StringUtil


class JsonMatcher(ABC):
    """Abstract base class for all JSON matchers.

    JSON Matchers are used to filter the results of a database query on
    client-side.

    See also c8y_api._base.CumulocityResource._iterate
    """

    def __init__(self, expression: str, warn_on_error: bool = False):
        self.expression = expression
        self.log = logging.getLogger('c8y_api.model.matcher')
        self.warn_on_error = warn_on_error

    def __repr__(self):
        return f'<{self.__class__.__name__} "{self.expression}">'

    @abstractmethod
    def matches(self, json: dict) -> bool:
        """Check if a JSON document matches.

        Args:
            json (dict): JSON document.

        Returns:
            True if the expression of this matcher matches the JSON document.
            False otherwise or if the expression could not be evaluated.
        """

    def safe_matches(self, json: dict) -> bool:
        """Check if a JSON document matches and log a warning if an
        exception occurs."""
        # pylint: disable=broad-exception-caught
        try:
            return self.matches(json)
        except Exception as error:
            self.log.warning(f"Matching \"{self.expression}\" failed with error: {error}")
            return False


class AllMatcher(JsonMatcher):
    """Higher level matcher matching if all the enclosed matchers match."""

    def __init__(self, *matchers: JsonMatcher):
        super().__init__(' AND '.join(str(m) for m in matchers))
        self.matchers = matchers

    def matches(self, json: dict) -> bool:
        return all(m.matches(json) for m in self.matchers)


def match_all(*matchers: JsonMatcher) -> AllMatcher:
    """Create a higher level matcher matching if all the enclosed matchers match."""
    return AllMatcher(*matchers)


class AnyMatcher(JsonMatcher):
    """Higher level matcher matching if any of the enclosed matcher matches."""

    def __init__(self, *matchers: JsonMatcher):
        super().__init__(' OR '.join(str(m) for m in matchers))
        self.matchers = matchers

    def matches(self, json: dict) -> bool:
        return any(m.matches(json) for m in self.matchers)


def match_any(*matchers: JsonMatcher) -> AnyMatcher:
    """Create a higher level matcher matching if any of the enclosed matcher matches."""
    return AnyMatcher(*matchers)


class NotMatcher(JsonMatcher):
    """Higher level matcher matching if the enclosed matcher doesn't match."""

    def __init__(self, matcher: JsonMatcher):
        super().__init__(f'NOT {matcher}')
        self.matcher = matcher

    def matches(self, json: dict) -> bool:
        return not self.matcher.matches(json)


def match_not(matcher: JsonMatcher) -> NotMatcher:
    """Create a higher level matcher matching if the enclosed matcher doesn't match."""
    return NotMatcher(matcher)


class FragmentMatcher(JsonMatcher):
    """Matcher matching the existence of a top-level fragment."""

    def __init__(self, name: str):
        super().__init__(name)

    def matches(self, json: dict) -> bool:
        return self.expression in json


def fragment(name: str) -> FragmentMatcher:
    """Create a matcher matching the existence of a top-level fragment."""
    return FragmentMatcher(name)


class FieldMatcher(JsonMatcher):
    """Generic matcher matching the value of a top-level string field."""

    class Mode:
        """The mode of matching."""
        LIKE = 'LIKE'
        REGEX = 'REGEX'

    def __init__(self, name: str, expression: str, mode: str = 'LIKE'):
        super().__init__(expression)
        self.field_name = name
        self.mode = mode

    def matches(self, json: dict) -> bool:
        return self.field_name in json and (
                (self.mode == 'REGEX' and _StringUtil.matches(self.expression, json[self.field_name])) or
                _StringUtil.like(self.expression, json[self.field_name])
        )


def field(name: str, value: str, mode: str = FieldMatcher.Mode.LIKE) -> FieldMatcher:
    """Create a matcher matching the value of a top-level string field."""
    return FieldMatcher(name, value, mode)


class DescriptionMatcher(FieldMatcher):
    """Matcher matching the top-level `description` field of a document."""

    def __init__(self, expression: str, mode: str = 'LIKE'):
        super().__init__('description', expression, mode)


def description(name: str, mode: str = FieldMatcher.Mode.LIKE) -> DescriptionMatcher:
    """Create a matcher matching the top-level `description` field of a document."""
    return DescriptionMatcher(name, mode)


class TextMatcher(FieldMatcher):
    """Matcher matching the top-level `text` field of a document."""

    def __init__(self, expression: str, mode: str = 'LIKE'):
        super().__init__('text', expression, mode)


def text(name: str, mode: str = FieldMatcher.Mode.LIKE) -> TextMatcher:
    """Create a matcher matching the top-level `text` field of a document."""
    return TextMatcher(name, mode)


class CommandMatcher(FieldMatcher):
    """Matcher matching the `text` field c8y_Command fragment."""

    def __init__(self, command_text: str, mode: str ='LIKE'):
        super().__init__('text', expression=command_text, mode=mode)

    def matches(self, json: dict) -> bool:
        return 'c8y_Command' in json and super().matches(json['c8y_Command'])


def command(name: str, mode: str = FieldMatcher.Mode.LIKE) -> CommandMatcher:
    """Create a matcher matching the `text` field c8y_Command fragment."""
    return CommandMatcher(name, mode)
