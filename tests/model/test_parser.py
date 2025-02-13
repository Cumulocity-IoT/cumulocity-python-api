# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=redefined-outer-name, protected-access

import random
import pytest

from c8y_api.model._parser import SimpleObjectParser

from util.testing_util import RandomNameGenerator


@pytest.fixture(scope='function')
def simple_object_and_mapping():
    """Provide a simple object instance and corresponding mapping definition
    for simple parser tests."""
    class TestClass:
        """Encapsulating test data."""
        def __init__(self):
            self.int_field = random.randint(1, 100)
            self.string_field = RandomNameGenerator.random_name()
            self.boolean_field = False
            self.additional = RandomNameGenerator.random_name()

    mapping = {
        'string_field': 'db_string',
        'int_field': 'db_int',
        'boolean_field': 'db_boolean',
        'special_field': 'db_special'}

    return TestClass(), mapping


def test_from_json_simple(simple_object_and_mapping):
    """Verify that parsing a JSON structure works using the from_json method
    of a parser instance."""

    obj, mapping = simple_object_and_mapping
    parser = SimpleObjectParser(mapping)

    source_json = {
        'to_be_ignored_field': 123,
        'db_int': random.randint(100, 200),
        'db_string': RandomNameGenerator.random_name(),
        'db_boolean': True,
        'db_special': "Special chars: ,._#'`\"?$%7{",
        'to_be_ignored_fragment': {'level': 2}}

    parsed_obj = parser.from_json(source_json, obj)

    assert parsed_obj.int_field == source_json['db_int']
    assert parsed_obj.string_field == source_json['db_string']
    assert parsed_obj.boolean_field == source_json['db_boolean']
    assert parsed_obj.special_field == source_json['db_special']


def test_from_json_simple_skip(simple_object_and_mapping):
    """Verify that parsing a JSON structure works using the from_json method
    with the skip parameter of a parser instance."""

    obj, mapping = simple_object_and_mapping
    parser = SimpleObjectParser(mapping)

    source_json = {'db_int': 12345}

    old_value = obj.int_field
    parsed_obj = parser.from_json(source_json, obj, skip=['int_field'])

    # the original field value should not change
    assert parsed_obj.int_field == old_value


def test_to_json_simple(simple_object_and_mapping):
    """Verify that simple JSON rendering works using the to_json method
    of a parser instance.

    When parsing we only want the known fields to be rendered."""

    obj, mapping = simple_object_and_mapping
    parser = SimpleObjectParser(mapping)

    target_json = parser.to_json(obj)

    # the object field values must be rendered correctly
    assert target_json['db_string'] == obj.string_field
    assert target_json['db_int'] == obj.int_field
    assert target_json['db_boolean'] == obj.boolean_field

    # exactly our expected fields, the object class defines an additional
    # field which should not be in our target JSON structure
    expected = {'db_string', 'db_int', 'db_boolean'}
    actual = set(target_json.keys())
    assert not actual ^ expected


def test_to_json_simple_includes(simple_object_and_mapping):
    """Verify that simple JSON rendering works using the to_json method
    of a parser instance when an include is specified."""

    obj, mapping = simple_object_and_mapping
    parser = SimpleObjectParser(mapping)

    obj.boolean_field = False
    target_json = parser.to_json(obj, include=['int_field', 'boolean_field'])

    # we expect only one field in the target json
    assert len(target_json) == 2

    # the object field values must be rendered correctly
    assert target_json['db_int'] == obj.int_field
    assert target_json['db_boolean'] == obj.boolean_field


def test_to_json_simple_excludes(simple_object_and_mapping):
    """Verify that simple JSON rendering works using the to_json method
    of a parser instance when an exclude list is specified."""

    obj, mapping = simple_object_and_mapping
    parser = SimpleObjectParser(mapping)

    obj.boolean_field = False
    target_json = parser.to_json(obj, exclude=['string_field'])

    # we expect only one field in the target json
    assert len(target_json) == 2

    # the object field values must be rendered correctly
    assert target_json['db_int'] == obj.int_field
    assert target_json['db_boolean'] == obj.boolean_field


def test_to_json_simple_overlap(simple_object_and_mapping):
    """Verify that simple JSON rendering works using the to_json method
    of a parser instance when both include and exclude list are specified
    and overlapping."""

    obj, mapping = simple_object_and_mapping
    parser = SimpleObjectParser(mapping)

    target_json = parser.to_json(obj, include=['int_field', 'string_field'], exclude=['string_field'])

    # we expect only one field in the target json
    assert len(target_json) == 1

    # the object field values must be rendered correctly
    assert target_json['db_int'] == obj.int_field
