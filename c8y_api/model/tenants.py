# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from __future__ import annotations

from datetime import datetime
from typing import Generator, List

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model.applications import Application
from c8y_api.model._base import SimpleObject, CumulocityResource
from c8y_api.model._parser import SimpleObjectParser


class Tenant(SimpleObject):
    """ Represent a tenant within the database.

    Instances of this class are returned by functions of the corresponding
    Tenant API. Use this class to create new or update objects.

    See also https://cumulocity.com/api/core/#tag/Tenants
    """
    _resource = '/tenant/tenants'
    _parser = SimpleObjectParser(
        creation_time='creationTime',
        _u_domain="domain",
        _u_admin_email="adminEmail",
        _u_admin_name="adminName",
        _u_admin_pass="adminPass",
        _u_company="company",
        _u_contact_name="contactName",
        _u_contact_phone="contactPhone",
        parent="parent",
        status="status",
    )
    _accept = 'application/vnd.com.nsn.cumulocity.tenant+json'

    def __init__(self,
                 c8y: CumulocityRestApi = None,
                 domain: str = None,
                 admin_email: str = None,
                 admin_name: str = None,
                 admin_pass: str = None,
                 company: str = None,
                 contact_name: str = None,
                 contact_phone: str = None,
                 ):
        """ Create a new Tenant instance.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            domain (str):
            admin_email (str):
            admin_name (str):
            admin_pass (str):
            company (str):
            contact_name (str):
            contact_phone (str):

        Returns:
             A new Tenant instance.
        """
        super().__init__(c8y)
        self._u_domain = domain
        self._u_admin_email = admin_email
        self._u_admin_name = admin_name
        self._u_admin_pass = admin_pass
        self._u_company = company
        self._u_contact_name = contact_name
        self._u_contact_phone = contact_phone
        self.creation_time = None
        self.parent = None
        self.status = None
        self._applications = []
        self._owned_applications = []

    domain = SimpleObject.UpdatableProperty('_u_domain')
    admin_name = SimpleObject.UpdatableProperty('_u_admin_name')
    admin_email = SimpleObject.UpdatableProperty('_u_admin_email')
    admin_pass = SimpleObject.UpdatableProperty('_u_admin_pass')
    company = SimpleObject.UpdatableProperty('_u_company')
    contact_name = SimpleObject.UpdatableProperty('_u_contact_name')
    contact_phone = SimpleObject.UpdatableProperty('_u_contact_phone')

    @property
    def creation_datetime(self) -> datetime:
        """Convert the tenant's creation time to a Python datetime object.

        Returns:
            Standard Python datetime object
        """
        return super()._to_datetime(self.creation_time)

    @property
    def applications(self) -> Generator[Application]:
        """Yield referenced applications.

        Returns:
            A Generator for Application instances.
        """
        for application_json in self._applications:
            yield Application.from_json(application_json)

    @property
    def all_applications(self) -> List[Application]:
        """Yield referenced applications.

        Returns:
            A list of Application instances.
        """
        return list(self.applications)

    @property
    def owned_applications(self) -> Generator[Application]:
        """Yield owned applications.

        Returns:
            A Generator for Application instances.
        """
        for application_json in self._owned_applications:
            yield Application.from_json(application_json)

    @property
    def all_owned_applications(self) -> List[Application]:
        """Yield owned application references.

        Returns:
            A list of Application instances.
        """
        return list(self.owned_applications)

    @classmethod
    def from_json(cls, json: dict) -> Tenant:
        """ Build a new Tenant instance from JSON.

        The JSON is assumed to be in the format as it is used by the
        Cumulocity REST API.

        Args:
            json (dict): JSON object (nested dictionary)
                representing a tenant option  within Cumulocity

        Returns:
            Tenant object
        """
        obj = cls._from_json(json, Tenant())
        # Extract (but don't parse) referenced application. Parsing is
        # done lazily in property implementations
        # pylint: disable=protected-access
        if 'applications' in json:
            obj._applications = [x['application'] for x in json['applications']['references']]
        if 'ownedApplications' in json:
            obj._owned_applications = [x['application'] for x in json['ownedApplications']['references']]
        return obj

    def create(self) -> Tenant:
        """ Create a new representation of this option within the database.

        Returns:
            A fresh Tenant instance representing the created
            option within the database.

        See also function `Tenants.create` which doesn't parse the result.
        """
        return super()._create()

    def update(self) -> Tenant:
        """ Write changes to the database.

        Returns:
            A fresh Tenant instance representing the updated
            object within the database.

        See also function `Tenants.update` which doesn't parse the result.
        """
        return super()._update()

    def delete(self) -> None:
        """Delete the tenant within the database.

        See also function `Tenants.delete` to delete multiple objects.
        """
        super()._delete()


class Tenants(CumulocityResource):
    """Provides access to the Tenant Options API.

    This class can be used for get, search for, create, update and
    delete tenant options within the Cumulocity database.

    See also: https://cumulocity.com/api/core/#tag/Tenants
    """

    def __init__(self, c8y: CumulocityRestApi):
        super().__init__(c8y, '/tenant/tenants')

    def get_current(self) -> Tenant:
        """Retrieve current tenant.

        Returns:
            Tenant instance
        """
        tenant = Tenant.from_json(self.c8y.get('/tenant/currentTenant'))
        tenant.c8y = self.c8y
        return tenant

    def get(self, tenant_id: str) -> Tenant:
        """Read a specific tenant from the database.

        Args:
            tenant_id (str|int):  database ID of a tenant

        Returns:
            Tenant object

        Raises:
            KeyError:  if the ID cannot be resolved.
        """
        tenant = Tenant.from_json(self._get_object(tenant_id))
        tenant.c8y = self.c8y  # inject c8y connection into instance
        return tenant

    def select(
            self,
            expression: str = None,
            parent: str = None,
            domain: str = None,
            company: str = None,
            limit: int = None,
            page_size: int = 1000,
            page_number: int = None,
            **kwargs
    ) -> Generator[Tenant]:
        """ Query the database for tenants and iterate over the results.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification.  Filters can be
        combined (within reason).

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            parent (str): ID of the parent tenant
            domain (str):  Tenant domain
            company (str): Tenant's assigned company name
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of objects which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator for Tenant instances
        """
        base_query = self._prepare_query(
            expression=expression,
            parent=parent,
            domain=domain,
            company=company,
            page_size=page_size,
            **kwargs)
        return super()._iterate(base_query, page_number, limit, Tenant.from_json)

    def get_all(
            self,
            expression: str = None,
            parent: str = None,
            domain: str = None,
            company: str = None,
            limit: int = None,
            page_size: int = 1000,
            page_number: int = None,
            **kwargs
    ) -> List[Tenant]:
        """ Query the database for tenants and return the results as list.

        All parameters are considered to be filters, limiting the result set
        to objects which meet the filters specification.  Filters can be
        combined (within reason).

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            parent (str): ID of the parent tenant
            domain (str):  Tenant domain
            company (str): Tenant's assigned company name
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of objects which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            List of Tenant instances
        """
        return list(self.select(
            expression=expression,
            parent=parent,
            domain=domain,
            company=company,
            limit=limit,
            page_size=page_size,
            page_number=page_number,
            **kwargs
        ))
