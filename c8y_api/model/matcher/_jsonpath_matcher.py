# Copyright (c) 2025 Cumulocity GmbH

from jsonpath_ng.ext import parse

from c8y_api.model.matcher._matcher import JsonMatcher


class JsonPathMatcher(JsonMatcher):
    """JsonMatcher implementation for JSONPath."""

    def __init__(self, expression: str, warn_on_error: bool = True):
        super().__init__(expression, warn_on_error)
        self.compiled_expression = parse(expression)

    def matches(self, json: dict) -> bool:
        # pylint: disable=broad-exception-caught
        return self.compiled_expression.find(json)


def jsonpath(expression: str) -> JsonPathMatcher:
    """Create a JMESPathMatcher from an expression."""
    return JsonPathMatcher(expression)
