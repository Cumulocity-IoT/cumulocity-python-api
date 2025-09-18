# Copyright (c) 2025 Cumulocity GmbH

from jsonpath_ng.ext import parse

from c8y_api.model.matcher._matcher import JsonMatcher


class JsonPathMatcher(JsonMatcher):
    """JsonMatcher implementation for JSONPath."""

    def __init__(self, expression: str, warn_on_error: bool = True):
        super().__init__(expression)
        self.warn_on_error = warn_on_error
        self.compiled_expression = parse(expression)

    def matches(self, json: dict) -> bool:
        # pylint: disable=broad-exception-caught
        try:
            return self.compiled_expression.find(json)
        except Exception as e:
            if self.warn_on_error:
                super()._log_eval_error(e, json)
            return False
