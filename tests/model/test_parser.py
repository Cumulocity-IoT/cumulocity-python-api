# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

import random
import pytest

from tests.utils import RandomNameGenerator

# pylint: disable=protected-access
from c8y_api.model._parser import SimpleObjectParser  # noqa


@pytest.fixture(scope='function')
def simple_object_and_mapping():
    """Provide a simple object instance and corresponding mapping definition
    for simple parser tests."""
    class TestClass:
        def __init__(self):
            self.int_field = random.randint(1, 100)
            self.string_field = RandomNameGenerator.random_name()
            self.boolean_field = True
            self.additional = RandomNameGenerator.random_name()

    mapping = {
        'string_field': 'db_string',
        'int_field': 'db_int'}

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
        'to_be_ignored_fragment': {'level': 2}}

    parsed_obj = parser.from_json(source_json, obj)

    assert parsed_obj.int_field == source_json['db_int']
    assert parsed_obj.string_field == source_json['db_string']


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

    # exactly our expected fields, the object class defines an additional
    # field which should not be in our target JSON structure
    expected = {'db_string', 'db_int'}
    actual = set(target_json.keys())
    assert not actual ^ expected


def test_to_json_simple_includes(simple_object_and_mapping):
    """Verify that simple JSON rendering works using the to_json method
    of a parser instance when an include is is specified."""

    obj, mapping = simple_object_and_mapping
    parser = SimpleObjectParser(mapping)

    target_json = parser.to_json(obj, include=['int_field'])

    # we expect only one field in the target json
    assert len(target_json) == 1

    # the object field values must be rendered correctly
    assert target_json['db_int'] == obj.int_field


def test_to_json_simple_excludes(simple_object_and_mapping):
    """Verify that simple JSON rendering works using the to_json method
    of a parser instance when an exclude list is specified."""

    obj, mapping = simple_object_and_mapping
    parser = SimpleObjectParser(mapping)

    target_json = parser.to_json(obj, exclude=['string_field'])

    # we expect only one field in the target json
    assert len(target_json) == 1

    # the object field values must be rendered correctly
    assert target_json['db_int'] == obj.int_field


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
