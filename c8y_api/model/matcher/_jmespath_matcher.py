# Copyright (c) 2025 Cumulocity GmbH

import jmespath

from c8y_api.model.matcher._matcher import JsonMatcher


class JmesPathMatcher(JsonMatcher):
    """JsonMatcher implementation for JMESPath."""

    def __init__(self, expression: str, warn_on_error: bool = True):
        super().__init__(expression)
        self.warn_on_error = warn_on_error
        self.compiled_expression = jmespath.compile(expression)

    def matches(self, json: dict) -> bool:
        # pylint: disable=broad-exception-caught
        try:
            return self.compiled_expression.search(json)
        except Exception as e:
            if self.warn_on_error:
                super()._log_eval_error(e, json)
            return False
