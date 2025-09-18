from ._matcher import JsonMatcher

__all__ = ['JsonMatcher']

try:
    import jmespath as _jmespath
    from ._jmespath_matcher import JmesPathMatcher

    def jmespath(expression: str) -> JmesPathMatcher:
        """Create a JMESPathMatcher from an expression."""
        return JmesPathMatcher(expression)

    __all__.append('JmesPathMatcher')
    __all__.append('jmespath')
except ImportError:
    pass

try:
    import jsonpath_ng
    from ._jsonpath_matcher import JsonPathMatcher

    def jsonpath(expression: str) -> JsonPathMatcher:
        """Create a JMESPathMatcher from an expression."""
        return JsonPathMatcher(expression)

    __all__.append('JsonPathMatcher')
    __all__.append('jsonpath')
except ImportError:
    pass
