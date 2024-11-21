# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

from typing import Generator, List

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model._base import SimpleObject, CumulocityResource
from c8y_api.model._parser import SimpleObjectParser


class TenantOption(SimpleObject):
    """ Represent a tenant option within the database.

    Instances of this class are returned by functions of the corresponding
    Tenant Option API. Use this class to create new or update options.

    See also https://cumulocity.com/guides/latest/reference/tenants/#option
    """
    # these need to be defined like this for the abstract super functions
    _resource = '/tenant/options'
    _parser = SimpleObjectParser({
        'category': 'category',
        'key': 'key',
        '_u_value': 'value'})
    _accept = 'application/vnd.com.nsn.cumulocity.option+json'

    def __init__(self, c8y: CumulocityRestApi = None, category: str = None, key: str = None, value: str = None):
        """ Create a new TenantOption instance.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            category (str): Option category
            key (str): Option key (name)
            value (str): Option value

        Returns:
            TenantObject instance
        """
        super().__init__(c8y)
        self.category = category
        self.key = key
        self._u_value = value

    value = SimpleObject.UpdatableProperty('_u_value')

    def _assert_id(self):
        if self.key is None or self.category is None:
            raise ValueError("Both option category abd key must be set to allow direct object access.")

    def _build_object_path(self):
        # no need to assert category/key this function is only used when a
        # corresponding assertion was run beforehand
        return f'{self._build_resource_path()}/{self.category}/{self.key}'

    @classmethod
    def from_json(cls, json: dict) -> TenantOption:
        """ Build a new TenantOption instance from JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Args:
            json (dict): JSON object (nested dictionary)
                representing a tenant option  within Cumulocity

        Returns:
            TenantOption object
        """
        return cls._from_json(json, TenantOption())

    def create(self) -> TenantOption:
        """ Create a new representation of this option within the database.

        Returns:
            A fresh TenantOption instance representing the created
            option within the database.

        See also function `TenantOptions.create` which doesn't parse the result.
        """
        return super()._create()

    def update(self) -> TenantOption:
        """ Write changes to the database.

        Returns:
            A fresh TenantOption instance representing the updated
            object within the database.

        See also function `TenantOptions.update` which doesn't parse the result.
        """
        return super()._update()

    def delete(self) -> None:
        """Delete the option within the database.

        See also function `TenantOptions.delete` to delete multiple objects.
        """
        super()._delete()


class TenantOptions(CumulocityResource):
    """Provides access to the Tenant Options API.

    This class can be used for get, search for, create, update and
    delete tenant options within the Cumulocity database.

    See also: https://cumulocity.com/api/latest/#tag/Options
    """

    def __init__(self, c8y: CumulocityRestApi):
        super().__init__(c8y, '/tenant/options')

    def build_object_path(self, category: str, key: str) -> str:  # noqa
        # pylint: disable=arguments-differ
        """Build the path to a specific object of this resource.

        Note: this function overrides with different arguments because
        tenant options, unlike other objects, are not identified by ID
        but category/key.

        Args:
            category (str):  Option category
            key (str): Option key (name)

        Returns:
            The relative path to the object within Cumulocity.
        """
        return f'{self.resource}/{category}/{key}'

    def select(self, category: str = None, limit: int = None,
               page_size: int = 1000, page_number: int = None) -> Generator[TenantOption]:
        """ Query the database for tenant options and iterate over the
        results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification.  Filters can be
        combined (within reason).

        Args:
            category (str):  Option category
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of objects which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator for TenantObject instances
        """
        if not category:
            # select all
            base_query = self._prepare_query(page_size=page_size)
            yield from super()._iterate(base_query, page_number, limit, TenantOption.from_json)
        else:
            # select by category, this is just a single request
            options_json = self.c8y.get(f'{self.resource}/{category}')
            for key, value in options_json.items():
                result = TenantOption(category=category, key=key, value=value)
                result.c8y = self.c8y  # inject c8y connection into instance
                yield result

    def get_all(self, category: str = None, limit: int = None,
                page_size: int = 1000, page_number: int = None) -> List[TenantOption]:
        """ Query the database for tenant options and return the results
        as list.

        This function is a greedy version of the `select` function. All
        available results are read immediately and returned as list.

        Returns:
            List of TenantObject instances
        """
        return list(self.select(category=category, limit=limit, page_size=page_size, page_number=page_number))

    def get_all_mapped(self, category: str = None) -> dict[str, str]:
        """ Query the database for tenant options and return the results
        as a dictionary.

        This result dictionary does not specify option categories hence
        it is best used with the category filter unless the keys are
        unique by themselves.

        Args:
            category (str):  Option category

        Returns:
            Dictionary of option keys to values.
        """
        return {o.key: o.value for o in self.get_all(category=category)}

    def get(self, category: str, key: str) -> TenantOption:
        """ Retrieve a specific option from the database.

        Args:
            category (str): Option category
            key (str): Option key (name)

        Returns:
             A TenantOption instance

        Raises:
            KeyError:  if the given combination of category and key
            is not defined within the database
        """
        option = TenantOption.from_json(self.c8y.get(resource=self.build_object_path(category, key)))
        option.c8y = self.c8y  # inject c8y connection into instance
        return option

    def get_value(self, category: str, key: str) -> str:
        """ Retrieve the value of a specific option from the database.

        Args:
            category (str): Option category
            key (str): Option key (name)

        Returns:
             The value of the specified option

        Raises:
            KeyError:  if the given combination of category and key
            is not defined within the database
        """
        # this is a very simple payload, we extract it directly
        return self.c8y.get(resource=self.build_object_path(category, key))['value']

    def set_value(self, category: str, key: str, value: str):
        """ Create an option within the database.

        This is a shortcut function to avoid unnecessary instantiation of
        the TenantOption class.

        Args:
            category (str): Option category
            key (str): Option key (name)
            value (str): Option value
        """
        self.create(TenantOption(category=category, key=key, value=value))

    def create(self, *options: TenantOption) -> None:
        """ Create options within the database.

        Args:
            *options (TenantOption):  Collection of TenantObject instances
        """
        super()._create(TenantOption.to_json, *options)

    def update(self, *options: TenantOption) -> None:
        """ Update options within the database.

        Args:
            *options (TenantOption):  Collection of TenantObject instances
        """
        for o in options:
            self.c8y.put(self.build_object_path(o.category, o.key), json=o.to_diff_json(), accept=None)

    def update_by(self, category: str, options: dict[str, str]) -> None:
        """ Update options within the database.

        Args:
            category (str):  Option category
            options (dict):  A dictionary of option keys and values
        """
        self.c8y.put(resource=f'{self.resource}/{category}', json=options, accept=None)

    def delete(self, *options: TenantOption) -> None:
        """ Delete options within the database.

        Args:
            *options (TenantOption):  Collection of TenantObject instances
        """
        for o in options:
            self.delete_by(o.category, o.key)

    def delete_by(self, category: str, key: str) -> None:
        """ Delete specific option within the database.

        Args:
            category (str):  Option category
            key (str):  Option key (name)
        """
        self.c8y.delete(self.build_object_path(category, key))
