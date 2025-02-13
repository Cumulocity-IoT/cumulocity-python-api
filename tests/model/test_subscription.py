# Copyright (c) 2025 Cumulocity GmbH

import json
import os

from typing import List
from unittest.mock import Mock

import pytest

from c8y_api import CumulocityRestApi
from c8y_api.model.notification2 import Subscription

from tests.utils import isolate_last_call_arg


def fix_sample_jsons() -> List[dict]:
    """Read sample jsons from file. This is not a pytest fixture."""
    path = os.path.dirname(__file__) + '/subscriptions.json'
    with open(path, encoding='utf-8', mode='rt') as f:
        subscriptions = json.load(f)
        return subscriptions['subscriptions']


@pytest.mark.parametrize('sample_json', fix_sample_jsons())
def test_parsing(sample_json):
    """Verify that parsing a Subscription from JSON works as expected."""
    subscription = Subscription.from_json(sample_json)

    assert subscription.name == sample_json['subscription']
    assert subscription.context == sample_json['context']
    assert subscription.source_id == sample_json['source']['id']
    if 'nonPersistent' in sample_json:
        assert subscription.non_persistent == sample_json['nonPersistent']
    if 'fragmentsToCopy' in sample_json:
        assert subscription.fragments == sample_json['fragmentsToCopy']
    if 'subscriptionFilter' in sample_json:
        subscription_filter = sample_json['subscriptionFilter']
        if 'apis' in subscription_filter:
            assert subscription.api_filter == subscription_filter['apis']
        if 'typeFilter' in subscription_filter:
            assert subscription.type_filter == subscription_filter['typeFilter']


def test_formatting():
    """Verify that JSON formatting works as expected."""

    subscription = Subscription(name='name', source_id='source_id', context=Subscription.Context.TENANT)

    subscription_json = subscription.to_full_json()
    assert subscription_json['subscription'] == 'name'
    assert subscription_json['context'] == 'tenant'
    assert subscription_json['source']['id'] == 'source_id'
    # expect no other entries
    assert len(subscription_json) == 3

    subscription.fragments = ['f1', 'f2']
    subscription.type_filter = 'type_filter'
    subscription_json = subscription.to_full_json()
    assert subscription_json['fragmentsToCopy'] == subscription.fragments
    assert subscription_json['subscriptionFilter']['typeFilter'] == subscription.type_filter
    # expect no other entries
    assert len(subscription_json) == 5

    subscription.api_filter = ['a1', 'a2']
    subscription.non_persistent = True
    subscription_json = subscription.to_full_json()
    assert subscription_json['fragmentsToCopy'] == subscription.fragments
    assert subscription_json['subscriptionFilter']['typeFilter'] == subscription.type_filter
    assert subscription_json['subscriptionFilter']['apis'] == subscription.api_filter
    assert subscription_json['nonPersistent']
    # expect no other entries
    assert len(subscription_json) == 6


def test_create():
    """Verify that the object creation works as expected (JSON & URLs)."""
    c8y: CumulocityRestApi = Mock()
    # the post function can just return any valid JSON
    sample_json = fix_sample_jsons()[0]
    c8y.post = Mock(return_value=sample_json)
    subscription = Subscription(c8y=c8y, name='name', source_id='source_id', context=Subscription.Context.TENANT)
    # overwrite the to_json to return known data
    subscription.to_json = Mock(return_value={'expected': True})

    updated_subscription = subscription.create()

    # 1) to_json should have been called
    assert subscription.to_json.call_count == 1
    # 2) the given resource path should be correct
    resource = isolate_last_call_arg(c8y.post, 'resource', 0)
    assert resource == '/notification2/subscriptions'
    # 3) the given payload should match what to_json returned
    payload = isolate_last_call_arg(c8y.post, 'json', 1)
    assert set(payload.keys()) == {'expected'}
    # 4) the return should be parsed properly
    assert updated_subscription.name == sample_json['subscription']
    assert updated_subscription.context == sample_json['context']
    assert updated_subscription.source_id == sample_json['source']['id']


def test_delete():
    """Verify that the object deletion works as expected."""
    c8y: CumulocityRestApi = Mock()
    c8y.delete = Mock()
    subscription = Subscription(c8y=c8y, name='name', source_id='source_id', context=Subscription.Context.TENANT)
    subscription.id = 'subscription-id'

    subscription.delete()

    assert c8y.delete.call_count == 1
    # 2) the given resource path should be correct
    resource = isolate_last_call_arg(c8y.delete, 'resource', 0)
    assert resource == f'/notification2/subscriptions/{subscription.id}'
