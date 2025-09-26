import logging
import re
from unittest.mock import patch, Mock

import pytest

from c8y_api.model.matcher import JsonMatcher, jmespath, jsonpath
from c8y_api.model.matcher import command, description, field, fragment, match_all, match_any, text, FieldMatcher


class MatchingMatcher(JsonMatcher):
    """Always matching matcher for test purposes."""
    def matches(self, _):
        return True


class NotMatchingMatcher(JsonMatcher):
    """Never matching matcher for test purposes."""
    def matches(self, _):
        return False


MATCH = MatchingMatcher('MATCH')
DONT_MATCH = NotMatchingMatcher('DONT_MATCH')


def test_logging(caplog):
    """Verify that exceptions during 'safe' matching are propagated
    as expected."""

    class FailingMatcher(JsonMatcher):
        """Always failing matcher for test purposes."""
        def matches(self, _):
            raise ValueError('expected')

    # 1) single matcher raises exception
    with pytest.raises(ValueError):
        FailingMatcher('FAIL').matches({})
    assert not caplog.records

    # 2) safe_matches returns False and warns
    with caplog.at_level(logging.WARNING):
        FailingMatcher('FAIL').safe_matches({})
    assert len(caplog.records) == 1
    r0 = caplog.records[0]
    assert r0.name == 'c8y_api.model.matcher'
    assert r0.levelname == 'WARNING'
    assert 'FAIL' in r0.message

    # 3) nested matcher propagates
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        match_all(MATCH, MATCH, FailingMatcher('FAIL')).safe_matches({})
    assert len(caplog.records) == 1
    assert re.search(r'MATCH.*AND.*MATCH.*AND.*FAIL', caplog.messages[0])


def test_fragment_matcher():
    """Verify that the fragment matchers work as expected."""
    assert fragment('fragment').matches({'fragment': {}})
    assert not fragment('fragment').matches({'other': {}})


@patch('c8y_api.model.matcher._matcher._StringUtil.like')
@patch('c8y_api.model.matcher._matcher._StringUtil.matches')
def test_field_matcher(matches_mock, like_mock):
    """Verify that description matchers work as expected.

    The field matcher can work in LIKE and REGEX mode, only one of the
    respective string util functions are expected to be invoked per
    matching attempt. If the field is not present in the JSON, no
    matching attempt is expected.
    """

    valid = {'field': 'text'}
    not_valid = {'other': 'text'}

    # field present, only like matcher is invoked
    like_mock.return_value = True
    assert field('field', 'expr').matches(valid)
    like_mock.assert_called_once_with('expr', 'text')

    # field present, only like matcher is invoked although not matching
    like_mock.reset_mock()
    like_mock.return_value = False
    assert not field('field', 'expr').matches(valid)
    like_mock.assert_called_once_with('expr', 'text')
    matches_mock.assert_not_called()

    # field not present, no matcher invoked
    like_mock.reset_mock()
    assert not field('field', 'expr').matches(not_valid)
    like_mock.assert_not_called()
    matches_mock.assert_not_called()

    # regex mode, like matcher not invoked
    assert field('field', 'expr', mode='REGEX').matches(valid)
    like_mock.assert_not_called()
    matches_mock.assert_called_once_with('expr', 'text')


def test_all_matcher():
    """Verify that ALL matchers work as expected.

    All the enclosed matchers are invoked until one fails.
    """
    match1 = Mock(matches=Mock(return_value=True))
    match2 = Mock(matches=Mock(return_value=True))
    dont_match = Mock(matches=Mock(return_value=False))
    match3 = Mock(matches=Mock(return_value=True))

    assert not match_all(match1, match2, dont_match, match3).matches({})
    match1.matches.assert_called_once_with({})
    match2.matches.assert_called_once_with({})
    dont_match.matches.assert_called_once_with({})
    match3.matches.assert_not_called()


def test_any_matcher():
    """Verify that ALL matchers work as expected.

    All the enclosed matchers are invoked until one matches.
    """
    match1 = Mock(matches=Mock(return_value=True))
    match2 = Mock(matches=Mock(return_value=True))
    dont_match = Mock(matches=Mock(return_value=False))

    assert match_any(dont_match, match1, match2).matches({})
    dont_match.matches.assert_called_once_with({})
    match1.matches.assert_called_once_with({})
    match2.matches.assert_not_called()

def test_description_matcher():
    """Verify that the description matchers are initialized correctly."""
    matcher = description('MATCH')
    assert isinstance(matcher, FieldMatcher)
    assert matcher.field_name == 'description'
    assert matcher.expression == 'MATCH'


def test_text_matcher():
    """Verify that the text matchers are initialized correctly."""
    matcher = text('MATCH')
    assert isinstance(matcher, FieldMatcher)
    assert matcher.field_name == 'text'
    assert matcher.expression == 'MATCH'


def test_command_matcher():
    """Verify that the text matchers work as expected.

    The command matcher is a regular field matcher, but the matched `text`
    field is nested within a `c8y_Command` fragment.
    """
    matcher = command('MATCH')
    with patch.object(FieldMatcher, 'matches') as matches_mock:
        matches_mock.return_value = True
        assert matcher.matches({'c8y_Command': 'random'})
        matches_mock.assert_called_once_with('random')

        matches_mock.reset_mock()
        assert not matcher.matches({'c8y_Other': 'random'})
        matches_mock.assert_not_called()


def test_jmespath_matcher():
    """Verify that the jmespath matchers work as expected."""
    assert jmespath("name == 'NAME'").matches({'name': 'NAME'})
    assert not jmespath("name == 'NAME'").matches({'name': 'RANDOM'})
    with pytest.raises(Exception) as error:
        jmespath("*INVALID*").matches({})
    assert "INVALID" in str(error.value)

# $[?(@.firstName == "John")]

def test_jsonpath_matcher():
    """Verify that the jsonpath matchers work as expected."""
    assert jsonpath('$.array[?(@ == 0)]').matches({'array': [0, 1, 2]})
    assert not jsonpath('$.array[?(@ == 0)]').matches({})
    with pytest.raises(Exception) as error:
        jsonpath("*INVALID*").matches({})
    assert "INVALID" in str(error.value)
