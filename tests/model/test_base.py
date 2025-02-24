# Copyright (c) 2025 Cumulocity GmbH

# pylint: disable=protected-access

from __future__ import annotations

import random
from unittest.mock import Mock

import pytest

from c8y_api import CumulocityRestApi
from c8y_api.model._base import SimpleObject, ComplexObject, CumulocityResource, CumulocityObject, get_by_path
from c8y_api.model._parser import SimpleObjectParser, ComplexObjectParser


class SimpleTestObject(SimpleObject):
    """A SimpleObject class to sample inheritance."""

    _parser = SimpleObjectParser({'_field': 'c8y_field', 'fixed_field': 'c8y_fixed'})

    def __init__(self, c8y: CumulocityRestApi = None, field: str = None, fixed_field: int = None):
        super().__init__(c8y=c8y)
        self._field = field
        self.fixed_field = fixed_field

    field = SimpleObject.UpdatableProperty('_field')


class ComplexTestObject(ComplexObject):
    """A ComplexObject class to sample inheritance."""

    _parser = ComplexObjectParser({'_field': 'c8y_field', 'fixed_field': 'c8y_fixed'}, ['c8y_ignored'])

    def __init__(self, c8y: CumulocityRestApi = None, field: str = None, fixed_field: int = None, **kwargs):
        super().__init__(c8y=c8y, **kwargs)
        self._field = field
        self.fixed_field = fixed_field

    field = SimpleObject.UpdatableProperty('_field')


@pytest.mark.parametrize("json, path, default, expected",[
    ({}, "some", None, None),
    ({'a': 1}, 'a', 'x', 1),
    ({'x': 1}, 'a', 1, 1),
    ({'a': 1, 'b': 2}, 'b', None, 2),
    ({'a': {'b': 1, 'c': 2}, 'm': '3'}, 'a.b', None, 1),
    ({'a': {'b': 1, 'c': 2}, 'm': '3'}, 'a.c', None, 2),
    ({'a': {'b': 1, 'c': 2}, 'm': 3}, 'm', None, 3),
    ({'a': {'b': 1, 'c': 2}, 'm': 3}, 'a.d', 4, 4),
])
def test_get_by_path(json, path, default, expected):
    """Verify that get by path works as expected."""
    assert get_by_path(json, path, default) == expected


def test_simpleobject_instantiation_and_formatting():
    """Verify that instantiation, basic attribute access and JSON formatting
     works as expected."""

    # 1_ when using the constructor and when setting standard attributes
    # no change will be recorded.

    obj = SimpleTestObject(field='field data')
    obj.id = '12'

    #  -> the properties are set
    assert obj.id == '12'
    assert obj.field == 'field data'
    #  -> the change set is undefined/empty
    assert not obj._updated_fields

    # 2_ when accessing the updatable field directly (like the parser) would
    # again, no change will be recorded.

    obj.__dict__['_field'] = 'directly updated field'

    #  -> the properties are set
    assert obj.field == 'directly updated field'
    #  -> the change set is undefined/empty
    assert not obj._updated_fields
    #  -> the updated JSON representation will be empty
    # pylint: disable=(use-implicit-booleaness-not-comparison
    assert obj._to_json(only_updated=True) == {}
    #  -> the full JSON representation will be defined
    assert obj._to_json(only_updated=False) == {'c8y_field': 'directly updated field'}

    # 3_ when updating the property via the descriptor (default access)
    # the change will be recorded.

    obj.field = 'new field data'

    #  -> the properties are set
    assert obj.field == 'new field data'
    #  -> the change set is updated accordingly
    assert obj._updated_fields == {'_field'}

    #  -> the full and diff JSON representation will be identical
    assert obj._to_json() == obj._to_json(only_updated=True)


def test_simpleobject_parsing():
    """Verify that parsing/formatting works as expected."""

    obj_json = {
        'id': 'some id (not mentioned in class, but should be parsed)',
        'self': 'some reference (should be ignored)',
        'c8y_field': 'field data',
        'c8y_fixed': 12
    }

    parsed_obj = SimpleTestObject._from_json(obj_json, SimpleTestObject())

    assert parsed_obj.id == obj_json['id']
    assert parsed_obj.field == obj_json['c8y_field']
    assert parsed_obj.fixed_field == obj_json['c8y_fixed']

    expected_json = {
        'c8y_field': parsed_obj.field,
        'c8y_fixed': parsed_obj.fixed_field
    }
    assert parsed_obj._to_json() == expected_json

    # 2_ when updating fields manually it will reflect in the diff JSON
    parsed_obj.id = 12
    parsed_obj.field = 'new field data'
    parsed_obj.fixed_field = 123

    expected_updated_json = {
        'c8y_field': parsed_obj.field,
        'c8y_fixed': parsed_obj.fixed_field
    }
    assert parsed_obj._to_json() == expected_updated_json

    expected_diff_json = {
        'c8y_field': parsed_obj.field
    }
    assert parsed_obj._to_json(only_updated=True) == expected_diff_json


def test_complexobject_parsing():
    """Verify that complex object parsing works as expected."""

    obj_json = {
        'id': 'some id (not mentioned in class, but should be parsed)',
        'self': 'some reference (should be ignored)',
        'c8y_field': 'field data',
        'c8y_fixed': 12,
        'c8y_ignored': True,
        'c8y_simple': 'simple attribute like fragment',
        'c8y_complex': {'field': 'value'}
    }

    # 1_ parsing the object JSON into a new object instance
    parsed_obj = ComplexTestObject._from_json(obj_json, ComplexTestObject())

    # -> all standard properties are set
    assert parsed_obj.id == obj_json['id']
    assert parsed_obj.field == obj_json['c8y_field']
    assert parsed_obj.fixed_field == obj_json['c8y_fixed']
    # -> the ignored fragment/elements are not defined
    assert not parsed_obj.has('self')
    assert not parsed_obj.has('c8y_ignored')
    # -> all fragments are set
    assert parsed_obj.c8y_simple == obj_json['c8y_simple']
    assert parsed_obj.c8y_complex.field == obj_json['c8y_complex']['field']
    # -> no update should be recorded
    assert not parsed_obj._updated_fields
    assert not parsed_obj._updated_fragments


def test_complexobject_instantiation_and_formatting():
    """Verify that complex object instantiation, basic access and JSON
    export works as expected."""

    # 1_ when using the constructor and standard functions, the
    # write access is not recorded
    obj = ComplexTestObject(
        field='field value',
        fixed_field=123,
        c8y_simple=True,
        c8y_complex={'a': 'valueA', 'b': 'valueB'},
        additionalField=True,
        additionalFragment={'value1': "A", 'value2': "B"}
    )

    # -> all standard properties are set
    assert obj.id is None
    assert obj.field == 'field value'
    assert obj.fixed_field == 123
    # -> all fragments are set
    assert obj.c8y_simple is True
    assert obj.c8y_complex.a == 'valueA'
    assert obj.c8y_complex.b == 'valueB'
    assert obj.additionalField is True
    assert obj.additionalFragment.value1 == 'A'
    assert obj.additionalFragment.value2 == 'B'
    # -> using snake_case access should also be allowed
    assert obj.additional_field is True
    assert obj.additional_fragment.value1 == 'A'
    assert obj.additional_fragment.value2 == 'B'
    # -> no update should be recorded
    assert not obj._updated_fields
    assert not obj._updated_fragments

    # 2_ when this is formatted as JSON, only the fragments will be
    # included in the diff JSON
    expected_full_json = {
        'c8y_field': obj.field,
        'c8y_fixed': obj.fixed_field,
        'c8y_simple': True,
        'c8y_complex': {'a': 'valueA', 'b': 'valueB'},
        'additionalField': True,
        'additionalFragment': {'value1': "A", 'value2': "B"}
    }
    # -> full JSON should contain all fields
    assert obj._to_json() == expected_full_json
    # -> diff JSON should be empty as there are no changes
    assert not obj.get_updates()
    # pylint: disable=(use-implicit-booleaness-not-comparison
    assert obj._to_json(only_updated=True) == {}

    # 3_ resetting the update status (twiddling with internals)
    obj._updated_fragments = None

    obj.field = 'updated field'
    obj['c8y_simple'] = False  # currently, direct setting of simple fragments is not supported
    obj.c8y_complex.b = 'newB'
    obj['additional_field'] = False  # snake case will be converted if there is a fitting field
    obj['another_field'] = True  # this will just be inserted as-is
    obj.additional_fragment.value1 = 'AA'
    obj.additionalFragment.value2 = 'BB'

    # -> the diff JSON should only contain updated parts
    expected_diff_json = {
        'c8y_field': obj.field,
        'c8y_simple': obj.c8y_simple,
        'c8y_complex': {'a': 'valueA', 'b': 'newB'},  # the 'a' field is unchanged but is included nonetheless
        'additionalField': False,
        'another_field': True,
        'additionalFragment': {'value1': 'AA', 'value2': 'BB'}
    }
    assert obj._to_json(only_updated=True) == expected_diff_json


class ComplexObjectUpdates:
    # pylint: disable=missing-class-docstring, missing-function-docstring

    @staticmethod
    def field(obj):
        obj.fragment.field = 'new value'
        assert obj.fragment.field == 'new value'

    @staticmethod
    def field2(obj):
        obj['fragment']['field'] = 'new value'
        assert obj.fragment.field == 'new value'

    @staticmethod
    def array(obj):
        obj.fragment.array = ['a', 'b']
        assert list(obj.fragment.array) == ['a', 'b']

    @staticmethod
    def array2(obj):
        obj['fragment']['array'] = ['a', 'b']
        assert list(obj.fragment.array) == ['a', 'b']

    @staticmethod
    def array_index(obj):
        obj.fragment.array[0] = 'c'
        assert list(obj.fragment.array) == ['c', 'b']

    @staticmethod
    def array_index2(obj):
        obj['fragment']['array'][0] = 'c'
        assert list(obj.fragment.array) == ['c', 'b']

    @staticmethod
    def array_append(obj):
        obj.fragment.array.append('x')
        assert list(obj.fragment.array) == ['a', 'b', 'x']

    @staticmethod
    def array_append2(obj):
        obj['fragment']['array'].append('x')
        assert list(obj.fragment.array) == ['a', 'b', 'x']

    @staticmethod
    def array_insert(obj):
        obj.fragment.array.insert(0, 'y')
        assert list(obj.fragment.array) == ['y', 'a', 'b']

    @staticmethod
    def array_insert2(obj):
        obj['fragment']['array'].insert(0, 'y')
        assert list(obj.fragment.array) == ['y', 'a', 'b']

    @staticmethod
    def array_extend(obj):
        obj.fragment.array.extend(['a'])
        assert list(obj.fragment.array) == ['a', 'b', 'a']

    @staticmethod
    def array_extend2(obj):
        obj['fragment']['array'].extend(['a'])
        assert list(obj.fragment.array) == ['a', 'b', 'a']


@pytest.mark.parametrize('fun', [
    ComplexObjectUpdates.field,
    ComplexObjectUpdates.field2,
    ComplexObjectUpdates.array,
    ComplexObjectUpdates.array2,
    ComplexObjectUpdates.array_index,
    ComplexObjectUpdates.array_index2,
    ComplexObjectUpdates.array_append,
    ComplexObjectUpdates.array_append2,
    ComplexObjectUpdates.array_extend,
    ComplexObjectUpdates.array_extend2,
])
def test_complex_object_updating(fun):
    """Verify that updating a complex object works as expected."""
    obj = ComplexObject(
        c8y=Mock(),
        fragment={
            'field': 'value',
            'array': ['a', 'b'],
        }
    )
    obj._signal_updated_fragment = Mock()
    fun(obj)
    obj._signal_updated_fragment.assert_called_with('fragment')


def test_complex_object_get():
    """Verify that get by path works as expected."""

    obj = ComplexTestObject(
        field='field value',
        fixed_field=123,
        c8y_simple=True,
        c8y_complex={'a': 'valueA', 'b': 'valueB'},
        additionalField=True,
        additionalFragment={'value1': "A", 'value2': "B"}
    )

    assert obj.get('field') == obj.field
    assert obj.get('c8y_complex.a') == obj.c8y_complex.a
    assert obj.get('not') is None
    assert obj.get('not', 'default') == 'default'
    assert obj.get('c8y_complex.not') is None
    assert obj.get('c8y_complex.not', default='default') == 'default'


path_options = [
    ('value', 'value'),
    (('value', 'x'), 'value'),
    ('not', None),
    (('not', None), None),
    (('not', []), []),
    (('not', True), True),
    (('not', False), False),
    ('empty', []),
    (('empty', None,), []),
    (('empty', []), []),
    (('empty', False), []),
    ('true', True),
    (('true', True), True),
    (('true', False), True),
    ('false', False),
    (('false', False), False),
    (('false', True), False),
    ('c8y_complex.a', 'valueA'),
    (('c8y_complex.a', None), 'valueA'),
    (('c8y_complex.a', True), 'valueA'),
    (('c8y_complex.a', False), 'valueA'),
    (('c8y_complex.a', []), 'valueA'),
    ('c8y_complex.b', []),
    (('c8y_complex.b', None), []),
    (('c8y_complex.b', True), []),
    (('c8y_complex.b', False), []),
    (('c8y_complex.b', []), []),
    ('c8y_complex.c', False),
    (('c8y_complex.c', None), False),
    (('c8y_complex.c', True), False),
    (('c8y_complex.c', False), False),
    (('c8y_complex.c', []), False),
    ('c8y_complex.not', None),
    (('c8y_complex.not', None), None),
    (('c8y_complex.not', '-'), '-'),
    (('c8y_complex.not', True), True),
    (('c8y_complex.not', False), False),
    (('c8y_complex.not', []), []),
]


@pytest.mark.parametrize('f1, e1', random.sample(path_options, 10))
@pytest.mark.parametrize('f2, e2', random.sample(path_options, 10))
@pytest.mark.parametrize('f3, e3', random.sample(path_options, 10))
def test_complex_object_as_tuples(f1, e1, f2, e2, f3, e3):
    """Verify that the as_tuples function works as expected."""

    obj = ComplexTestObject(
        value='value',
        empty=[],
        true=True,
        false=False,
        c8y_complex={'a': 'valueA', 'b': [], 'c': False},
    )

    # multiple values
    assert obj.as_tuple(f1, f2, f3) == (e1, e2, e3)


def test_dot_notation():
    """Verify that the dot notation access works as expected."""
    obj = ComplexTestObject(
        field='field value',
        array=['a', 'b'],
        nested={
            'as': ['a1', 'a2'],
            'bs': [
                {'n': 1,
                 'v': 'b1'},
                {'n': 2,
                 'v': 'b2'},
            ]},
        mixed=[
            'a',
            1,
            {'inner': 'field'},
        ]
    )

    assert obj.array[0] == 'a'
    assert obj.array[1] == 'b'
    assert obj.nested['as'][0] == 'a1'
    assert obj.nested['as'][1] == 'a2'
    assert obj.nested.bs[0].n == 1
    assert obj.nested.bs[0].v == 'b1'
    assert obj.nested.bs[1].n == 2
    assert obj.nested.bs[1].v == 'b2'
    assert obj.mixed[0] == 'a'
    assert obj.mixed[1] == 1
    assert obj.mixed[2].inner == 'field'


@pytest.mark.parametrize('page_size, num_all, limit, expected', [
    (10, 100, 100, 100),  # exact
    (10, 200, 100, 100),  # limit hit
    (10, 99, 100, 99),    # all
    (10, 1, 100, 1),      # just one
    (10, 9, 100, 9),      # first page
    (10, 11, 100, 11),    # second page
    (1, 10, 100, 10),     # min page size
    (1, 0, 100, 0),       # no results
])
def test_iteration(page_size, num_all, limit, expected):
    """Verify that iteration works as expected."""
    all_items = [{'i': i} for i in range(num_all)]

    # returns a 'page' from all items
    def get_page(_, p):
        nonlocal all_items
        s = page_size * (p - 1)
        e = page_size * p
        return all_items[s:e]

    # parses an item as CumulocityObject
    def parse_fun(item):
        obj = CumulocityObject(None)
        obj.id = item['i']
        return obj

    # create class under test
    res = CumulocityResource(Mock(CumulocityRestApi), '')
    res._get_page = Mock(side_effect=get_page)

    # iterate oder results
    result = list(res._iterate(base_query="q", page_number=None, limit=limit, parse_fun=parse_fun))
    result_ids = [x.id for x in result]

    # check expectation
    assert result_ids == list(range(expected))
