# Copyright (c) 2025 Cumulocity GmbH

from pydictdisplayfilter.display_filters import DictDisplayFilter

from c8y_api.model.matcher._matcher import JsonMatcher


class PydfMatcher(JsonMatcher):
    """JsonMatcher implementation for PyDF (Python Display Filter)."""
    # pylint: disable=protected-access

    def __init__(self, expression: str, warn_on_error: bool = True):
        super().__init__(expression, warn_on_error)
        self.display_filter = DictDisplayFilter([])
        self.compiled_expression = self.display_filter._display_filter_parser.parse(expression)

    def matches(self, json: dict) -> bool:
        # pylint: disable=broad-exception-caught
        return self.display_filter._evaluate_expressions(self.compiled_expression, json)


def pydf(expression: str) -> PydfMatcher:
    """Create a PydfMatcher from an expression."""
    return PydfMatcher(expression)
