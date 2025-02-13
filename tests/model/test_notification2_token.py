# Copyright (c) 2025 Cumulocity GmbH

from unittest.mock import Mock

import pytest

from c8y_api import CumulocityRestApi
from c8y_api.model.notification2 import Tokens

from util.testing_util import RandomNameGenerator
from tests.utils import isolate_last_call_arg


@pytest.mark.parametrize(
    'tls, consumer, expected', [
        (False, False, 'ws://c8y.com/notification2/consumer/?token={}'),
        (True, False, 'wss://c8y.com/notification2/consumer/?token={}'),
        (False, True, 'ws://c8y.com/notification2/consumer/?token={}&consumer={}'),
        (True, True, 'wss://c8y.com/notification2/consumer/?token={}&consumer={}'),
    ])
def test_uri_generator(tls, consumer, expected):
    """Verify that building the websocket URI works as expected."""

    token = RandomNameGenerator.random_name()
    consumer = RandomNameGenerator.random_name() if consumer else None

    protocol = 'https' if tls else 'http'
    c8y = CumulocityRestApi(base_url=f'{protocol}://c8y.com', tenant_id='t123', username='un', password='pw')
    uri = Tokens(c8y).build_websocket_uri(token, consumer if consumer else None)

    assert uri == expected.format(token, consumer)


@pytest.mark.parametrize(
    'subscription, expiry, subscriber, shared, signed, non_persistent', [
        ('sub', 123, 'id123', None, None, None),
        ('sub', 0, None, True, False, True),
        ('sub', 123, 'id123', False, True, False),
    ])
def test_generate(subscription, expiry, subscriber, shared, signed, non_persistent):
    """Verify that token generation works as expected."""

    c8y = CumulocityRestApi(base_url='https://c8y.com', tenant_id='t123', username='un', password='pw')
    c8y.post = Mock(return_value={'token': 'TOKEN'})
    Tokens(c8y).generate(subscription=subscription, expires=expiry, subscriber=subscriber,
                         shared=shared, signed=signed, non_persistent=non_persistent)
    td_json = isolate_last_call_arg(c8y.post, 'json', 1)

    assert td_json['subscription'] == subscription
    assert td_json['expiresInMinutes'] == expiry

    if subscriber:
        assert td_json['subscriber'] == subscriber
    else:
        assert 'c8yapi' in td_json['subscriber']

    if shared is not None:
        assert td_json['shared'] == shared
    if signed is not None:
        assert td_json['signed'] == signed
    if non_persistent is not None:
        assert td_json['nonPersistent'] == non_persistent
