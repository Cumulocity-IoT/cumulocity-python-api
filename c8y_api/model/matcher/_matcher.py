# Copyright (c) 2025 Cumulocity GmbH

import logging
from abc import ABC, abstractmethod


class JsonMatcher(ABC):
    """Abstract base class for all JSON matchers.

    JSON Matchers are used to filter the results of a database query on
    client-side.

    See also c8y_api._base.CumulocityResource._iterate
    """

    def __init__(self, expression: str):
        self.expression = expression
        self.log = logging.getLogger('c8y_api.model.matcher')

    def _log_eval_error(self, error, data):
        """Log an error during evaluation consistently across all matchers."""
        self.log.warning(f"Matching expression \"{self.expression}\" failed with error: {error}")
        print(f"Matching expression \"{self.expression}\" failed with error: {error}")

    def __repr__(self):
        return f'<{self.__class__.__name__} expression="{self.expression}">'

    @abstractmethod
    def matches(self, json: dict) -> bool:
        """Check if a JSON document matches.

        Args:
            json (dict): JSON document.

        Returns:
            True if the expression of this matcher matches the JSON document.
            False otherwise or if the expression could not be evaluated.
        """
