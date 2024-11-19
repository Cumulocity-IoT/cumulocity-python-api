# Copyright (c) 2021 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

import json as js
from typing import List, Set, Generator
import urllib.parse
import uuid

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model._base import SimpleObject, CumulocityResource
from c8y_api.model._parser import SimpleObjectParser


class Subscription(SimpleObject):
    """ Represent a Notification 2.0 subscription within the database.

    Instances of this class are returned by functions of the corresponding
    Subscriptions API. Use this class to create new options.

    See also: https://cumulocity.com/api/#tag/Subscriptions
    """

    class Context(object):
        """Notification context types."""
        MANAGED_OBJECT = 'mo'
        TENANT = 'tenant'

    class ApiFilter(object):
        """Notification API filter types."""
        ANY = '*'
        ALARMS = 'alarms'
        ALARMS_WITH_CHILDREN = 'alarmsWithChildren'
        EVENTS = 'events'
        EVENTS_WITH_CHILDREN = 'eventsWithChildren'
        MANAGED_OBJECTS = 'managedobjects'
        MEASUREMENTS = 'measurements'
        OPERATIONS = 'operations'

    _resource = '/notification2/subscriptions'
    _parser = SimpleObjectParser({
        'name': 'subscription',
        'context': 'context',
        'non_persistent': 'nonPersistent',
        'fragments': 'fragmentsToCopy'})
    _accept = 'application/vnd.com.nsn.cumulocity.subscription+json'

    def __init__(self, c8y: CumulocityRestApi = None, name: str = None, context: str = None, source_id: str = None,
                 api_filter: List[str] = None, type_filter: str = None,
                 fragments: List[str] = None, non_persistent: bool = None):
        """ Create a new Subscription instance.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            name (str):  Subscription name
            context (str):  Subscription context.
            source_id (str):  Managed object ID the subscription is for.
            api_filter (List[str]):  List of APIs/resources to subscribe for.
            type_filter (str):  Object type the subscription is for.
            non_persistent (bool):  Whether the subscription is non-persistent.

        Returns:
            Subscription instance
        """
        super().__init__(c8y)
        self.name = name
        self.context = context
        self.source_id = source_id
        self.non_persistent = non_persistent
        self.api_filter = api_filter
        self.type_filter = type_filter
        self.fragments = fragments

    def _to_json(self, only_updated=False, exclude: Set[str] = None) -> dict:
        json = super()._to_json(only_updated=only_updated, exclude=exclude)
        if self.source_id:
            json['source'] = {'id': self.source_id}
        if self.api_filter or self.type_filter:
            subscription_filter = {'apis': self.api_filter if self.api_filter else None,
                                   'typeFilter': self.type_filter if self.type_filter else None}
            json['subscriptionFilter'] = subscription_filter
        return json

    @classmethod
    def from_json(cls, json: dict) -> Subscription:
        """Create a Subscription instance from Cumulocity JSON format.

        Args:
            json (dict): The JSON to parse.

        Returns:
            A Subscription instance.
        """
        subscription = super()._from_json(json, Subscription())
        subscription.source_id = json['source']['id']
        if 'subscriptionFilter' in json:
            if 'apis' in json['subscriptionFilter']:
                subscription.api_filter = json['subscriptionFilter']['apis']
            if 'typeFilter' in json['subscriptionFilter']:
                subscription.type_filter = json['subscriptionFilter']['typeFilter']
        return subscription

    def create(self) -> Subscription:
        """ Create a new subscription within the database.

        Returns:
            A fresh Subscription instance representing the created
            subscription within the database.

        See also function `Subscriptions.create` which doesn't parse the result.
        """
        return self._create()


class Subscriptions(CumulocityResource):
    """Provides access to the Notification 2.0 Subscriptions API.

    This class can be used for get, search for, create, and
    delete Notification2 subscriptions within the Cumulocity database.

    See also: https://cumulocity.com/api/#tag/Subscriptions
              https://cumulocity.com/guides/reference/notifications/
    """

    def __init__(self, c8y: CumulocityRestApi):
        super().__init__(c8y, '/notification2/subscriptions')

    def get(self, subscription_id: str) -> Subscription:
        """ Retrieve a specific subscription from the database.

        Args:
            subscription_id (str):  Subscription ID

        Returns:
             A Subscription instance

        Raises:
            KeyError:  if the given ID is not defined within the database
        """
        subscription = Subscription.from_json(super()._get_object(subscription_id))
        subscription.c8y = self.c8y  # inject c8y connection into instance
        return subscription

    def get_count(
            self,
            expression: str = None,
            context: str = None,
            source: str = None,
            subscription: str = None,
            type_filter: str = None,
            **kwargs
    ) -> int:
        """Calculate the number of potential results of a database query.

        This function uses the same parameters as the `select` function.

        Returns:
            Number of potential results
        """
        base_query = self._prepare_query(
            expression=expression,
            context=context,
            source=source,
            subscription=subscription,
            typeFilter=type_filter,
            page_size=1,
            **kwargs
        )
        return self._get_count(base_query)

    def select(
            self,
            expression: str = None,
            context: str = None,
            source: str = None,
            subscription: str = None,
            type_filter: str = None,
            limit: int = None,
            page_size: int = 1000,
            page_number: int = None,
            **kwargs
    ) -> Generator[Subscription]:
        """ Query the database for subscriptions and iterate over the
        results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters' specification.  Filters can be
        combined (within reason).

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            context (str):  Subscription context.
            source (str):  Managed object ID the subscription is for.
            subscription (str): The subscription name.
            type_filter (str): The type to filter subscriptions
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of objects which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator for Subscription instances
        """
        base_query = self._prepare_query(
            expression=expression,
            context=context,
            source=source,
            subscription=subscription,
            typeFilter=type_filter,
            page_size=page_size,
            **kwargs)
        return super()._iterate(base_query, page_number, limit, Subscription.from_json)

    def get_all(
            self,
            expression: str = None,
            context: str = None,
            source: str = None,
            subscription: str = None,
            type_filter: str = None,
            limit: int = None,
            page_size: int = 1000,
            page_number: int = None,
            **kwargs
    ) -> List[Subscription]:
        """ Query the database for subscriptions and return the results
        as list.

        This function is a greedy version of the `select` function. All
        available results are read immediately and returned as list.

        Returns:
            List of Subscription instances.
        """
        return list(self.select(
            expression=expression,
            context=context, source=source, subscription=subscription, type_filter=type_filter,
            limit=limit, page_size=page_size, page_number=page_number,
            **kwargs))

    def create(self, *subscriptions: Subscription) -> None:
        """ Create subscriptions within the database.

        Args:
            *subscriptions (TenantOption):  Collection of Subscription instances
        """
        super()._create(Subscription.to_full_json, *subscriptions)

    def delete_by(self, context: str = None, source: str = None) -> None:
        """ Delete subscriptions within the database.

        Args:
            context (str):  Subscription context
            source (str):  Managed object ID the subscription is for.
        """
        self.c8y.delete(self._prepare_query(context=context, source=source))


class Tokens(CumulocityResource):
    """Provides access to the Notification 2.0 token generation API.

    This class can be used for get, search for, create, and
    delete Notification2 subscriptions within the Cumulocity database.

    See also: https://cumulocity.com/api/#tag/Tokens
              https://cumulocity.com/guides/reference/notifications/
    """

    _subscriber_uuid = uuid.uuid5(uuid.NAMESPACE_URL, 'https://github.com/SoftwareAG/cumulocity-python-api')
    _default_subscriber = 'c8yapi' + str(_subscriber_uuid).replace('-', '')

    def __init__(self, c8y: CumulocityRestApi):
        super().__init__(c8y, '/notification2')
        self.host = urllib.parse.urlparse(c8y.base_url).netloc

    def generate(self, subscription: str, expires: int = 60, subscriber: str = None,
                 signed: bool = None, shared: bool = None, non_persistent: bool = None) -> str:
        """Generate a new access token.

        Args:
            subscription (str): Subscription name.
            expires (int):  Expiration time in minutes.
            subscriber (str):  Subscriber ID (name). A UUID based default will be used if None.
            signed (bool):  Whether the token should be signed.
            shared (bool):  Whether the token is used to create a shared consumer.
            non_persistent (bool): Whether the token refers to the non-persistent variant of the named subscription.

        Returns:
            JWT access token as string.
        """
        td_json = self._build_token_definition(subscription, expires, subscriber, signed, shared, non_persistent)
        token_json = self.c8y.post(self.resource + '/token', td_json)
        return token_json['token']

    def renew(self, token: str) -> str:
        """Renew a token.

        Args:
            token:  Currently valid token to be renewed.
        """

    def unsubscribe(self, token: str):
        """Invalidate a token and unsubscribe a subscriber.

        Args:
            token (str):  Subscribed token
        """
        result_json = self.c8y.post(self.resource + '/unsubscribe?token=' + token, json={})
        if not result_json['result'] == 'DONE':
            raise RuntimeError(f"Unexpected response: {js.dumps(result_json)}")

    def build_websocket_uri(self, token: str, consumer: str = None):
        """Build websocket access URL.

        Args:
            token (str):  Subscriber access token
            consumer (str): Optional consumer ID (to allow 'sticky' connections after interrupt).

        Returns:
             A websocket (ws(s)://) URL to access the subscriber channel.
        """
        protocol = 'wss' if self.c8y.is_tls else 'ws'
        consumer_param = f'&consumer={consumer}' if consumer else ''
        return f'{protocol}://{self.host}/notification2/consumer/?token={token}{consumer_param}'

    def _build_token_definition(self, subscription: str, expires: int, subscriber: str = None,
                                signed=None, shared=None, non_persistent=None):
        json = {
            'subscriber': subscriber or self._default_subscriber,
            'subscription': subscription,
            'expiresInMinutes': expires,
        }
        if signed is not None:
            json['signed'] = signed
        if shared is not None:
            json['shared'] = shared
        if non_persistent is not None:
            json['nonPersistent'] = non_persistent
        return json
