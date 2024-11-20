# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

# pylint: disable=too-many-lines

from __future__ import annotations

from datetime import datetime
from typing import Generator, List

from c8y_api._base_api import CumulocityRestApi
from c8y_api.model._base import CumulocityResource, SimpleObject
from c8y_api.model._parser import SimpleObjectParser, ComplexObjectParser
from c8y_api.model._util import _DateUtil


class PermissionUtil:
    """Utility functions to work with the Permission API."""

    @staticmethod
    def build_reference(permission_id: str) -> dict:
        """Build the JSON for a Cumulocity reference to a permission."""
        # Luckily these references don't need the tenant ID
        return {'role': {'self': f'user/roles/{permission_id}'}}


class Permission(SimpleObject):
    """Represents a Permission object within Cumulocity.

    Notes:
      - Permissions are not created/deleted but only assigned to users or
        global roles

    See also: https://cumulocity.com/api/#tag/Roles
    """

    class Level(object):
        """Permission levels."""
        ANY = '*'
        READ = 'READ'
        WRITE = 'ADMIN'

    class Scope(object):
        """Permission scopes."""
        ANY = '*'
        ALARM = 'ALARM'
        AUDIT = 'AUDIT'
        EVENT = 'EVENT'
        MEASUREMENT = 'MEASUREMENT'
        MANAGED_OBJECT = 'MANAGED_OBJECT'
        OPERATION = 'OPERATION'

    _parser = SimpleObjectParser({
        'level': 'permission',
        'type': 'type',
        'scope': 'scope'})

    def __init__(self, level: str = Level.ANY, scope: str = Scope.ANY, type: str = '*'):
        """Create a new Permission instance.

        Args:
            level (str): One of ADMIN, READ, * (default)
            scope (str): One of ALARM, AUDIT, EVENT, MEASUREMENT,
                MANAGED_OBJECT, OPERATION, or * (default)
            type (str): Type on which to restrict or * (default)
        """
        super().__init__(c8y=None)
        self.level = level
        self.type = type
        self.scope = scope

    @classmethod
    def from_json(cls, json: dict) -> Permission:
        # no doc change required
        return cls._from_json(json, Permission())

    def to_json(self, only_updated=False) -> dict:
        # no doc change required
        json = self._to_json()
        # for permissions, it is actually ok to give the ID if there is any
        # for updates, this will create fewer objects within the database
        if self.id:
            # permission IDs are actually ints
            json['id'] = int(self.id)
        return json


class ReadPermission(Permission):
    """Represents a read permission within Cumulocity."""

    # pylint: disable=abstract-method
    def __init__(self, scope=Permission.Scope.ANY, type='*'):  # noqa
        super().__init__(level=Permission.Level.READ, scope=scope, type=type)


class WritePermission(Permission):
    """Represents a write permission within Cumulocity."""

    # pylint: disable=abstract-method
    def __init__(self, scope=Permission.Scope.ANY, type='*'):  # noqa
        super().__init__(level=Permission.Level.WRITE, scope=scope, type=type)


class AnyPermission(Permission):
    """Represents a read/write permission within Cumulocity."""

    # pylint: disable=abstract-method
    def __init__(self, scope=Permission.Scope.ANY, type='*'):  # noqa
        super().__init__(level=Permission.Level.ANY, scope=scope, type=type)


class InventoryRole(SimpleObject):
    """Represent an instance of an inventory role object in Cumulocity.

    Instances of this class are returned by functions of the corresponding
    Inventory Role API.
    Use this class to create new or update inventory role objects.

    See also: https://cumulocity.com/api/#tag/Inventory-Roles
    """

    _parser = SimpleObjectParser({
        '_u_name': 'name',
        '_u_description': 'description'})
    _resource = '/user/inventoryroles'

    def __init__(self, c8y: CumulocityRestApi = None, name: str = None, description: str = None,
                 permissions: List[Permission] = None):
        """Create a new InventoryRole instance.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            name (str):  Name of the inventory role
            description (str):  A description for the inventory role
            permissions (List[Permission]):  A collection of permissions that
                the inventory role assembles
        """
        super().__init__(c8y)
        self._u_name = name
        self._u_description = description
        self.permissions = permissions if permissions else []

    name = SimpleObject.UpdatableProperty('_u_name')
    description = SimpleObject.UpdatableProperty('_u_description')

    @classmethod
    def from_json(cls, json: dict) -> InventoryRole:
        # no doc change required
        obj = super()._from_json(json, InventoryRole())
        obj.permissions = list(map(lambda p: Permission.from_json(p), json['permissions']))
        return obj

    def to_json(self, only_updated=False) -> dict:
        # no doc change required
        json = super()._to_json(only_updated)
        json['permissions'] = list(map(lambda p: p.to_json(), self.permissions))
        return json

    def create(self) -> InventoryRole:
        """Create the role within the database.

        Returns:
            A fresh InventoryRole object representing what was
            created within the database (including the ID).
        """
        return super()._create()

    def update(self) -> InventoryRole:
        """Update the role within the database.

        Note: This will only send changed fields to increase performance.

        Returns:
            A fresh InventoryRole object representing the updated
            database state (including the ID).
        """
        return super()._update()

    def delete(self):
        """Delete the role within the database."""
        super()._delete()


class InventoryRoleAssignment(SimpleObject):
    """Represent an instance of an inventory role assignment in Cumulocity.

    This class is used internally by the InventoryRole and InventoryRoles
    classes. It cannot be used directly.

    See also: https://cumulocity.com/api/#tag/Inventory-Roles
    """
    _parser = SimpleObjectParser({
        'managed_object': 'managedObject'})

    def __init__(self, c8y: CumulocityRestApi = None, managed_object: str = None,
                 roles: List[InventoryRole] = None):
        """Create a new InventoryRoleAssignment instance.

        Args:
            c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                to be set for direct manipulation (create, delete)
            managed_object (str):  ID of the managed object for which the roles
                are assigned
            roles (List[InventoryRole]): List of inventory role objects to assign
        """
        super().__init__(c8y)
        self.managed_object = managed_object
        self.roles: List[InventoryRole] = roles if roles else []

    @classmethod
    def from_json(cls, json: dict) -> InventoryRoleAssignment:
        # no doc change required
        obj = cls._parser.from_json(json, InventoryRoleAssignment())
        obj.roles = list(map(lambda p: InventoryRole.from_json(p), json['roles']))
        return obj

    def to_json(self, only_updated=False) -> dict:
        # no doc change required
        j = super().to_json(only_updated)
        j['roles'] = list(map(lambda r: r.to_json(), self.roles))
        return j


class GlobalRole(SimpleObject):
    """Represents a Global Role object within Cumulocity.

    Notes:
      - Global Roles are called 'groups' in the Cumulocity Standard REST API;
        However, 'global roles' is the official concept name and therefore
        used for consistency with the Cumulocity realm.

      - Only a limited set of properties are actually updatable. Others must
        be set explicitly using the corresponding API (for example: permissions).

    See also: https://cumulocity.com/api/#tag/Groups
    """

    _parser = SimpleObjectParser({
        'id': 'id',
        '_u_name': 'name',
        '_u_description': 'description'})
    _resource = 'INVALID'  # needs to be dynamically generated. see _build_resource_path
    _accept = CumulocityRestApi.ACCEPT_GLOBAL_ROLE
    _custom_properties_parser = ComplexObjectParser({}, [])

    def __init__(self, c8y=None, name=None, description=None):
        super().__init__(c8y)
        self._u_name = name
        self._u_description = description
        self.permission_ids = set()
        self.application_ids = set()

    name = SimpleObject.UpdatableProperty('_u_name')
    description = SimpleObject.UpdatableProperty('_u_description')

    @classmethod
    def from_json(cls, json: dict) -> GlobalRole:
        # no doc change
        role: GlobalRole = cls._from_json(json, GlobalRole())
        # role ID are int for some reason - convert for consistency
        role.id = str(role.id)
        if json['roles'] and json['roles']['references']:
            role.permission_ids = {ref['role']['id'] for ref in json['roles']['references']}
        if json['applications']:
            role.application_ids = {ref['id'] for ref in json['applications']}
        return role

    # custom implementation for to_json not required

    def create(self) -> GlobalRole:
        """Create the GlobalRole within the database.

        Returns:
            A fresh GlobalRole object representing what was
            created within the database (including the ID).
        """
        return super()._create()

    def update(self) -> GlobalRole:
        """Update the GlobalRole within the database.

        Returns:
            A fresh GlobalRole object representing what the updated
            state within the database (including the ID).
        """
        return super()._update()

    def delete(self):
        """Delete the GlobalRole within the database."""
        super()._delete()

    def add_permissions(self, *permissions: str):
        """Add permissions to a global role.

        This operation is executed immediately.

        Args:
            *permissions (str):  An Iterable of permission ID
        """
        super()._assert_c8y()
        super()._assert_id()
        GlobalRoles(self.c8y).assign_permissions(self.id, *permissions)

    def remove_permissions(self, *permissions):
        """Remove permissions from a global role.

        This operation is executed immediately.

        Args:
            *permissions (str):  An Iterable of permission ID
        """
        super()._assert_c8y()
        super()._assert_id()
        GlobalRoles(self.c8y).unassign_permissions(self.id, *permissions)

    def add_users(self, *users: str):
        """Add users to a global role.

        This operation is executed immediately.

        Args:
            *users (str):  An Iterable of usernames
        """
        super()._assert_c8y()
        super()._assert_id()
        GlobalRoles(self.c8y).assign_users(self.id, *users)

    def remove_users(self, *users: str):
        """Remove users from a global role.

        This operation is executed immediately.

        Args:
            *users (str):  An Iterable of usernames
        """
        super()._assert_c8y()
        super()._assert_id()
        GlobalRoles(self.c8y).unassign_users(self.id, *users)

    def _build_resource_path(self):
        # overriding the default as we need the tenant ID in there
        return f'/user/{self.c8y.tenant_id}/groups'


class UserUtil:
    """Utility functions to work with the User API."""

    @staticmethod
    def build_user_reference(tenant_id: str, username: str) -> dict:
        """Build the JSON structure for a user reference."""
        return {'user': {'self': f'/user/{tenant_id}/users/{username}'}}

    @staticmethod
    def build_owner_reference(user_id: str) -> dict:
        """Build the JSON structure for an owner reference."""
        return {'owner': user_id}

    @staticmethod
    def build_delegate_reference(user_id: str) -> dict:
        """Build the JSON structure for a delegate reference."""
        return {'delegatedBy': user_id}

    @staticmethod
    def build_application_references(*ids) -> List[dict]:
        """Build the JSON structure for an application reference."""
        if not ids:
            return []
        return [{'id': str(aid), 'type': 'MICROSERVICE'} for aid in ids]

    @staticmethod
    def build_inventoryrole_assignment(object_id: int | str, *role_ids: int | str) -> dict:
        """Build the JSON structure for an inventory role assignment."""
        return {'managedObject': int(object_id), 'roles': [{'id': int(rid)} for rid in role_ids]}


class TfaSettings:
    """TFA settings representation within Cumulocity."""

    _parser = SimpleObjectParser(
        enabled='tfaEnabled',
        enforced='tfaEnforced',
        strategy='strategy',
        last_request_time='lastTfaRequestTime')

    def __init__(self,
                 enabled: bool = None,
                 enforced: bool = None,
                 strategy: str = None,
                 last_request_time: str | datetime = None):
        """Create a TfaSettings instance.

        Args:
            enabled (bool):  Whether TFA is enabled
            enforced (bool):  Whether TFA is enforced
            strategy (str):  TFA strategy, e.g. "SMS" or "TOTP"
            last_request_time (str|datetime):  The last time TFA was requested
        """
        self.enabled = enabled
        self.enforced = enforced
        self.strategy = strategy
        self.last_request_time = _DateUtil.ensure_timestring(last_request_time)

    @property
    def last_request_datetime(self) -> datetime:
        """Convert the last requests time to a Python datetime object.

        Returns:
            Standard Python datetime object
        """
        return _DateUtil.to_datetime(self.last_request_time)

    @classmethod
    def from_json(cls, object_json: dict) -> TfaSettings:
        """Create an object instance from Cumulocity JSON format.

        Caveat: this function is primarily for internal use and does not
        return a full representation of the JSON. It is used for object
        creation and update within Cumulocity.

        Args:
            object_json (dict): The JSON to parse.

        Returns:
            A TfaSettings instance.
        """
        return cls._parser.from_json(object_json, TfaSettings())

    def to_json(self) -> dict:
        """Create a representation of this object in Cumulocity JSON format.

        Caveat: this function is primarily for internal use and does not
        return a full representation of the object. It is used for object
        creation and update within Cumulocity.

        Returns:
            A JSON (nested dict) object.
        """
        return self._parser.to_json(self)


class _BaseUser(SimpleObject):
    _parser = SimpleObjectParser({
        'username': 'userName',
        'password_strength': 'passwordStrength',
        'owner': 'owner',
        'delegated_by': 'delegatedBy',
        '_u_email': 'email',
        '_u_enabled': 'enabled',
        '_u_display_name': 'displayName',
        '_u_password': 'password',
        '_u_first_name': 'firstName',
        '_u_last_name': 'lastName',
        '_u_tfa_enabled': 'twoFactorAuthenticationEnabled',
        '_u_require_password_reset': 'shouldResetPassword',
        '_password_reset_mail': 'sendPasswordResetEmail',
        '_last_password_change': 'lastPasswordChange'})
    _resource = 'INVALID'  # needs to be dynamically generated. see _build_resource_path

    def __init__(self, c8y: CumulocityRestApi = None, username: str = None, email: str = None,
                 enabled: bool = True, display_name: str = None, password: str = None,
                 first_name: str = None, last_name: str = None, phone: str = None,
                 tfa_enabled: bool = None, require_password_reset: bool = None):
        super().__init__(c8y)
        self.username = username
        self.password_strength = None
        self.owner = None
        self.delegated_by = None
        self._u_email = email
        self._u_enabled = enabled
        self._u_display_name = display_name
        self._u_password = password
        self._u_phone = phone
        self._u_first_name = first_name
        self._u_last_name = last_name
        self._u_tfa_enabled = tfa_enabled or False
        self._u_require_password_reset = require_password_reset
        self._password_reset_mail = not self._u_password
        self._last_password_change = None

    display_name = SimpleObject.UpdatableProperty('_u_display_name')
    email = SimpleObject.UpdatableProperty('_u_email')
    phone = SimpleObject.UpdatableProperty('_u_phone')
    first_name = SimpleObject.UpdatableProperty('_u_first_name')
    last_name = SimpleObject.UpdatableProperty('_u_last_name')
    enabled = SimpleObject.UpdatableProperty('_u_enabled')
    tfa_enabled = SimpleObject.UpdatableProperty('_u_tfa_enabled')
    require_password_reset = SimpleObject.UpdatableProperty('_u_require_password_reset')

    @property
    def last_password_change(self) -> datetime:
        """Get the last password change time."""
        # hint: could be cached, but it is rarely accessed multiple times
        return self._last_password_change

    @property
    def last_password_change_datetime(self) -> datetime:
        """Get the last password change time."""
        # hint: could be cached, but it is rarely accessed multiple times
        return _DateUtil.to_datetime(self._last_password_change)


class User(_BaseUser):
    """Represents a User object within Cumulocity.

    Notes:
      - Only a limited set of properties are actually updatable. Others must
        be set explicitly using the corresponding API (for example: global
        roles, permissions, owner, etc.)
    """
    _resource = 'INVALID'  # needs to be dynamically generated. see _build_resource_path
    _accept = CumulocityRestApi.ACCEPT_USER
    _custom_properties_parser = ComplexObjectParser({}, [])

    def __init__(self, c8y=None, username=None, email=None, enabled=True, display_name=None,
                 password=None, first_name=None, last_name=None, phone=None,
                 tfa_enabled=None, require_password_reset=None):
        """
            Create a new User instance.

            Args:
                c8y (CumulocityRestApi):  Cumulocity connection reference; needs
                    to be set for direct manipulation (create, delete).
                username (str):  The user's username.
                email (str):  The user's email address.
                enabled (bool):  Whether the user is enabled.
                display_name (str):  The user's display name
                password (str):  The initial password for the user. If omitted,
                    a newly created user will be sent a password reset link
                    (for human users).
                first_name (str):  The user's first name.
                last_name (str):  The user's last name.
                phone (str):  The user's phone number.
                tfa_enabled (bool):  Whether 2nd factor login is enabled.
                require_password_reset (bool):  Whether the password must be
                    reset by the user after the next login.
        """
        super().__init__(c8y,
                         username=username, email=email, enabled=enabled,
                         display_name=display_name, password=password,
                         first_name=first_name, last_name=last_name,
                         phone=phone, tfa_enabled=tfa_enabled,
                         require_password_reset=require_password_reset)
        self.global_role_ids = set()
        self.permission_ids = set()
        self.application_ids = set()
        # self.effective_permission_ids = set()
        # self.custom_properties = WithUpdatableFragments()

    @classmethod
    def from_json(cls, json: dict) -> User:
        user = cls._from_json(json, User())
        if 'groups' in json and 'references' in json['groups']:
            user.global_role_ids = {str(ref['group']['id']) for ref in json['groups']['references']}
        if 'roles' in json and 'references' in json['roles']:
            user.permission_ids = {ref['role']['id'] for ref in json['roles']['references']}
        if 'applications' in json:
            user.application_ids = {x['id'] for x in json['applications']}
        # if user_json['customProperties']:
        #     user.custom_properties = cls.__custom_properties_parser.from_json(user_json['customProperties'],
        #                                                                       WithUpdatableFragments())
        return user

    # no need to override the standard to_json method

    def create(self) -> User:
        """Create the User within the database.

        Returns:
            A fresh User object representing what was
            created within the database (including the ID).
        """
        return self._create()

    def update(self) -> User:
        """Update the User within the database.

        Returns:
            A fresh User object representing what the updated
            state within the database (including the ID).
        """
        # user update is not ID, but username based,
        # hence this custom implementation
        self._assert_c8y()
        self._assert_username()
        result_json = self.c8y.put(self._build_user_path(), self.to_diff_json(), accept=self._accept)
        return self.from_json(result_json)

    def delete(self):
        """Delete the User within the database."""
        self._delete()

    def set_owner(self, user_id: str):
        """Set the owner for this user.

        This operation is executed immediately. No additional call to
        the ``update`` function required.

        Args:
            user_id (str): ID of the owner to set; can be None to
                remove a currently set owner.
        """
        self._assert_c8y()
        self._assert_username()
        Users(self.c8y).set_owner(self.username, user_id)

    def set_delegate(self, user_id: str):
        """Set the delegate for this user.

        This operation is executed immediately. No additional call to
        the ``update`` function required.

        Args:
            user_id (str): ID of the delegate to set; can be ``None`` to
                remove a currently set delegate.
        """
        self._assert_c8y()
        self._assert_username()
        Users(self.c8y).set_delegate(self.username, user_id)

    def assign_global_role(self, role_id: str):
        """Assign a global role.

        This operation is executed immediately. No call to ``update``
        is required.

        Args:
            role_id (str): Object ID of an existing global role
        """
        self._assert_c8y()
        self._assert_username()
        GlobalRoles(self.c8y).assign_users(role_id, self.username)

    def unassign_global_role(self, role_id):
        """Unassign a global role.

        This operation is executed immediately. No call to ``update``
        is required.

        Args:
            role_id (str): Object ID of an assigned global role
        """
        self._assert_c8y()
        self._assert_username()
        GlobalRoles(self.c8y).unassign_users(role_id, self.username)

    def retrieve_global_roles(self) -> List[GlobalRole]:
        """Retrieve user's global roles.

        This operation is executed immediately. No call to ``update``
        is required.

        Returns:
            A list of assigned global roles.
        """
        self._assert_c8y()
        self._assert_username()
        return GlobalRoles(self.c8y).get_all(self.username)

    def retrieve_inventory_role_assignments(self):
        """Retrieve the user's inventory roles.

        This operation is executed immediately. No call to ``update``
        is required.

        Returns:
            A list of assigned inventory roles.
        """
        self._assert_c8y()
        self._assert_username()
        return InventoryRoles(self.c8y).get_all_assignments(self.username)

    def assign_inventory_roles(self, object_id: str | int, *roles: str | int | InventoryRole):
        """Assign an inventory role.

        This operation is executed immediately. No call to ``update``
        is required.

        Args:
            object_id (str): Object ID of an existing managed object
                (i.e. device group)
            *roles (str|int|InventoryRole): Existing InventoryRole objects resp.
                the ID of existing inventory roles
        """

        def resolve_role_ids(*rs: InventoryRole | int | str) -> List[int | str]:
            if isinstance(rs[0], InventoryRole):
                return [r.id for r in rs]
            return list(rs)

        self._assert_c8y()
        roles_path = self._build_user_path() + '/roles/inventory'
        assignment_json = UserUtil.build_inventoryrole_assignment(object_id, *resolve_role_ids(*roles))
        self.c8y.post(roles_path, assignment_json)

    def unassign_inventory_roles(self, *assignment_ids: str):
        """Unassign an inventory role.

        This operation is executed immediately. No call to ``update``
        is required.

        Args:
            *assignment_ids (str): Object ID of existing inventory role
                assignments (for this user)
        """
        base_path = self._build_user_path() + '/roles/inventory/'
        for aid in assignment_ids:
            self.c8y.delete(base_path + str(aid))

    def _build_resource_path(self):
        # overriding the default as we need the tenant ID in there
        return f'/user/{self.c8y.tenant_id}/users'

    def _build_user_path(self):
        return f'/user/{self.c8y.tenant_id}/users/{self.username}'

    def _build_object_path(self):
        # overriding the default as the username is the relevant ID
        return self._build_user_path()

    def _assert_username(self):
        if not self.username:
            raise ValueError("Username must be provided.")


class CurrentUser(_BaseUser):
    """Represents a "current" User object within Cumulocity.

    See also https://cumulocity.com/api/core/#tag/Current-User
    """

    class TotpActivity:
        """User's TOTP activity information."""

        def __init__(self, is_active: bool = None):
            self.is_active = is_active

        @classmethod
        def from_json(cls, object_json: dict) -> CurrentUser.TotpActivity:
            """Create an object instance from Cumulocity JSON format.

            Caveat: this function is primarily for internal use and does not
            return a full representation of the JSON. It is used for object
            creation and update within Cumulocity.

            Args:
                object_json (dict): The JSON to parse.

            Returns:
                A TotpActivity instance.
            """
            obj = CurrentUser.TotpActivity()
            obj.is_active = object_json['isActive']
            return obj

        def to_json(self) -> dict:
            """Create a representation of this object in Cumulocity JSON format.

            Caveat: this function is primarily for internal use and does not
            return a full representation of the object. It is used for object
            creation and update within Cumulocity.

            Returns:
                A JSON (nested dict) object.
            """
            return {'isActive': self.is_active}

    _resource = '/user/currentUser'
    _accept = CumulocityRestApi.ACCEPT_CURRENT_USER

    def __init__(self, c8y: CumulocityRestApi = None):
        super().__init__(c8y)
        self.effective_permission_ids = {}

    @classmethod
    def from_json(cls, json: dict) -> CurrentUser:
        user: CurrentUser = cls._from_json(json, CurrentUser())
        if 'effectiveRoles' in json:
            user.effective_permission_ids = {ref['id'] for ref in json['effectiveRoles']}
        return user

    # no need to override the standard to_json method

    def update(self) -> CurrentUser:
        """Update the current user within the database.

        Returns:
            A fresh CurrentUser object representing what the updated
            state within the database (including the ID).
        """
        self._assert_c8y()
        result_json = self.c8y.put(self._resource, self.to_diff_json(), accept=self._accept)
        user = self.from_json(result_json)
        user.c8y = self.c8y
        return user

    def update_password(self, current_password: str, new_password: str):
        """Update the current user's password:

        Args:
            current_password(str): the current password
            new_password (str): the new password to set
        """
        self._assert_c8y()
        Users(self.c8y).set_current_password(current_password, new_password)

    def get_tfa_settings(self) -> TfaSettings:
        """Read the TFA settings for the current user.

        Returns:
            A TfaSettings instance.
        """
        self._assert_c8y()
        return Users(self.c8y).get_tfa_settings(self.username)

    def _read_totp_activity(self) -> dict:
        self._assert_c8y()
        return self.c8y.get(f'{self._resource}/totpSecret/activity')

    def _write_totp_activity(self, activity_json: dict):
        self._assert_c8y()
        self.c8y.post(f'{self._resource}/totpSecret/activity', activity_json)

    def get_totp_activity(self) -> TotpActivity:
        """Read the TOTP activity details.

        Returns:
            A TotpActivity instance.
        """
        return CurrentUser.TotpActivity.from_json(self._read_totp_activity())

    def set_totp_activity(self, activity: TotpActivity):
        """Update the TFA feature activity details.

        Args:
            activity(TotpActivity): The TFA activity details.
        """
        self._write_totp_activity(activity.to_json())

    def get_totp_enabled(self) -> bool:
        """Check whether the TOTP feature is enabled for the current user.

        Returns:
            True if the feature is enabled, False otherwise.
        """
        try:
            return self._read_totp_activity()['isActive']
        except KeyError:
            return False

    def _set_totp_enabled(self, enabled: bool):
        """Enable/disable the TOTP feature for the current user.

        Args:
            enabled (bool): Whether to enable the feature.
        """
        self._write_totp_activity({'isActive': enabled})

    def enable_totp(self):
        """Enable the TOTP feature for the current user."""
        self._set_totp_enabled(True)

    def disable_totp(self):
        """Enable the TOTP feature for the current user."""
        self._set_totp_enabled(False)

    def generate_totp_secret(self) -> (str, str):
        """Generate a new TOTP secret for the current user.

        Returns:
            A (str, str) tuple of the raw secret token and the secret URL.
        """
        self._assert_c8y()
        result_json = self.c8y.post(f'{self._resource}/totpSecret', {})
        return result_json['rawSecret'], result_json['secretQrUrl']

    def verify_totp(self, code: str):
        """Verify a TFA/TOTP token.

        Args:
            code (str): A TOTP token

        Raises:
            ValueError if the token is invalid/could not be verified.
        """
        self._assert_c8y()
        self.c8y.post(f'{self._resource}/totpSecret/verify', {'code': code})

    def is_valid_totp(self, code: str) -> bool:
        """Verify a TFA/TOTP token.

        Args:
            code (str): A TOTP token

        Returns:
            True if the token was valid, False otherwise,
        """
        try:
            self.verify_totp(code)
            return True
        except ValueError:
            return False

    def revoke_totp_secret(self):
        """Revoke the currently set TFA/TOTP secret for the current user."""
        self._assert_c8y()
        Users(self.c8y).revoke_totp_secret(self.username)


class InventoryRoles(CumulocityResource):
    """Provides access to the InventoryRole API.

    This class can be used for get, search for, create, update and
    delete inventory roles within the Cumulocity database.

    See also: https://cumulocity.com/api/#tag/Inventory-Roles
    """

    def __init__(self, c8y):
        super().__init__(c8y, '/user/inventoryroles')
        self.object_name = "roles"

    def get(self, role_id: str | int) -> InventoryRole:
        """Get a specific inventory role object.

        Args:
            role_id (str|int): Cumulocity ID of the inventory role

        Returns:
            An InventoryRole instance for this ID

        Raises:
            SyntaxError:  if the ID is not defined.

        Note: In contrast to other API the InventoryRole API does not raise
        an KeyError (i.e. 404) for undefined ID but a SyntaxError (HTTP 500).
        """
        role = InventoryRole.from_json(self._get_object(role_id))
        role.c8y = self.c8y  # inject c8y connection into instance
        return role

    def select(self, limit: int = None, page_size: int = 1000, page_number: int = None) -> Generator[InventoryRole]:
        """Get all defined inventory roles.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        Note: The InventoryRole API does not support filters.

        Args:
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of objects read (and parsed
                in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator for InventoryRole objects
        """
        base_query = self._prepare_query(page_size=page_size)
        return super()._iterate(base_query, page_number, limit, InventoryRole.from_json)

    def get_all(self, limit: int = None, page_size: int = 1000, page_number: int = None) -> List[InventoryRole]:
        """Get all defined inventory roles.

        This function is a greedy version of the ``select`` function. All
        available results are read immediately and returned as list.

        See ``select`` for a documentation of arguments.

        Returns:
            List of InventoryRole objects
        """
        return list(self.select(limit=limit, page_size=page_size, page_number=page_number))

    def select_assignments(self, username: str) -> Generator[InventoryRoleAssignment]:
        """Get all inventory role assignments of a user.

        This function is implemented in a lazy fashion - results will only be
        fetched from the database as long there is a consumer for them.

        Args:
            username (str):  Username of a Cumulocity user

        Returns:
             Generator for InventoryRoleAssignment objects
        """
        query = f'/user/{self.c8y.tenant_id}/users/{username}/roles/inventory'
        assignments_json = self.c8y.get(query)
        for j in assignments_json['inventoryAssignments']:
            result = InventoryRoleAssignment.from_json(j)
            result.c8y = self.c8y  # inject c8y connection into instance
            yield result

    def get_all_assignments(self, username: str) -> List[InventoryRoleAssignment]:
        """Get all inventory role assignments of a user.

        This function is a greedy version of the ``select_assignments``
        function. All available results are read immediately and returned
        as list.

        See ``select_assignments`` for a documentation of arguments.

        Returns:
            List of InventoryRoleAssignment objects
        """
        return list(self.select_assignments(username))

    def create(self, *roles: InventoryRole):
        """Create objects within the database.

        Args:
            *roles (InventoryRole):  Collection of InventoryRole instances
        """
        super()._create(InventoryRole.to_full_json, *roles)

    def update(self, *roles: InventoryRole):
        """Write changes to the database.

        Args:
            *roles (InventoryRole):  Collection of InventoryRole instances
        """
        super()._update(InventoryRole.to_diff_json, *roles)


class Users(CumulocityResource):
    """Provides access to the User API.

    See also: https://cumulocity.com/api/#tag/Users
    """

    def __init__(self, c8y):
        super().__init__(c8y, 'user/' + c8y.tenant_id + '/users')
        self.__groups = GlobalRoles(c8y)

    def get(self, username: str):
        """Retrieve a specific user.

        Args:
            username (str): The ID of the user (usually the mail address)

        Returns:
            A User instance
        """
        user = User.from_json(self._get_object(username))
        user.c8y = self.c8y  # inject c8y connection into instance
        return user

    def get_current(self) -> CurrentUser:
        """Retrieve current user.

        Returns:
            CurrentUser instance
        """
        user = CurrentUser.from_json(self.c8y.get('/user/currentUser'))
        user.c8y = self.c8y
        return user

    def select(
            self,
            expression: str = None,
            username: str = None,
            groups: str | int | GlobalRole | List[str] | List[int] | List[GlobalRole] = None,
            owner: str = None,
            only_devices: bool = None,
            with_subusers_count: bool = None,
            limit: int = None,
            page_size: int = 5,
            page_number: int = None,
            **kwargs
    ) -> Generator[User]:
        """Lazily select and yield User instances.

        The result can be limited by username (prefix) and/or group membership.

        Args:
            expression (str):  Arbitrary filter expression which will be
                passed to Cumulocity without change; all other filters
                are ignored if this is provided
            username (str): A user's username or a prefix thereof
            groups (int, [int], str, [str], GlobalRole, [GlobalRole]): a scalar
                or list of int (actual group ID), string (group names), or actual
                Group instances
            owner (str):  Username of the owner of the user
            only_devices (bool):  Only return device users (starting with `device_`)
                If absent or False, the users will be excluded.
            with_subusers_count (bool):  Whether to include an additional field
             `subusersCount` which holds the number of direct sub users.
            limit (int): Limit the number of results to this number.
            page_size (int): Define the number of events which are read (and
                parsed in one chunk). This is a performance related setting.
            page_number (int): Pull a specific page; this effectively disables
                automatic follow-up page retrieval.

        Returns:
            Generator of User instances
        """
        # group_list can be ints, strings (names) or Group objects
        # it needs to become a comma-separated string
        groups_string = None
        if groups:  # either non-empty list or scalar
            # ensure it's a list to allow
            if not isinstance(groups, list):
                groups = [groups]
            if isinstance(groups[0], int):
                groups_string = [str(i) for i in groups]
            elif isinstance(groups[0], GlobalRole):
                groups_string = [str(g.id) for g in groups]
            elif isinstance(groups[0], str):
                groups_string = [str(self.__groups.get(name).id) for name in groups]
            else:
                raise ValueError("Unable to identify type of given group identifiers.")
            groups_string = ','.join(groups_string)
        # lazily yield parsed objects page by page
        base_query = super()._prepare_query(
            expression=expression,
            username=username,
            groups=groups_string,
            owner=owner,
            only_devices=only_devices,
            with_subusers_count=with_subusers_count,
            page_size=page_size,

            **kwargs
        )
        return super()._iterate(base_query, page_number, limit, User.from_json)

    def get_all(
            self,
            username: str = None,
            groups: str | int | GlobalRole | List[str] | List[int] | List[GlobalRole] = None,
            owner: str = None,
            only_devices: bool = None,
            with_subusers_count: bool = None,
            page_size: int = 1000,
            **kwargs
    ) -> list[User]:
        """Select and retrieve User instances as list.

        The result can be limited by username (prefix) and/or group membership.

        See `select` for a documentation of arguments.

        Returns:
            List of User instances
        """
        return list(self.select(
            username=username,
            groups=groups,
            owner=owner,
            only_devices=only_devices,
            with_subusers_count=with_subusers_count,
            page_size=page_size,
            **kwargs))

    def create(self, *users):
        """Create users within the database.

        Args:
            *users (User):  Collection of User instances
        """
        super()._create(lambda u: u.to_full_json(), *users)

    def logout_all(self):
        """Terminate all user's sessions."""
        self.c8y.post(f'/user/logout/{self.c8y.tenant_id}/allUsers', json={})

    def set_current_password(self, current_password: str, new_password: str):
        """Set the password of the current user.

        Note: This automatically updates the connection with the new auth information.

        Args:
            current_password (str): The current password
            new_password (str): The new password to set
        """
        request_json = {
            'currentUserPassword': current_password,
            'newPassword': new_password}
        self.c8y.put('/user/currentUser/password', request_json)
        self.c8y.auth.password = new_password

    def set_owner(self, user_id: str, owner_id: str | None):
        """Set the owner of a given user.

        Args:
            user_id (str): The user to set an owner for
            owner_id (str):  The ID of the owner user; Can be None to
                unassign/remove the current owner
        """
        if owner_id:
            self.c8y.put(self.build_object_path(user_id) + '/owner', UserUtil.build_owner_reference(owner_id))
        else:
            self.c8y.delete(self.build_object_path(user_id) + '/owner')

    def set_delegate(self, user_id: str, delegate_id: str | None):
        """Set the delegate of a given user.

        Args:
            user_id (str): The user to set an owner for
            delegate_id (str):  The ID of the delegate user; Can be None to
                unassign/remove the current owner
        """
        if delegate_id:
            self.c8y.put(self.build_object_path(user_id) + '/delegatedby',
                         UserUtil.build_delegate_reference(delegate_id))
        else:
            self.c8y.delete(self.build_object_path(user_id) + '/delegatedby')

    def get_tfa_settings(self, user_id: str) -> TfaSettings:
        """Read the TFA settings of a given user.

        Args:
            user_id (str): The user to query the settings for

        Returns:
            A TfaSettings object
        """
        return TfaSettings.from_json(self.c8y.get(self.build_object_path(user_id) + '/tfa'))

    def revoke_totp_secret(self, user_id: str):
        """Revoke the currently set TFA/TOTP secret for a user.

        Args:
            user_id (str): The user to set an owner for
        """
        self.c8y.delete(self.build_object_path(user_id) + '/totpSecret/revoke')


class GlobalRoles(CumulocityResource):
    """Provides access to the Global Role API.

    Notes:
      - Global Roles are called 'groups' in the Cumulocity Standard REST API;
        However, 'global roles' is the official concept name and therefore
        used for consistency with the Cumulocity realm.

    See also: https://cumulocity.com/api/#tag/Groups
    """

    def __init__(self, c8y):
        super().__init__(c8y, 'user/' + c8y.tenant_id + '/groups')
        self._global_roles_by_name = None

    def reset_caches(self):
        """Reset internal caching.

        Caches are used for lookups of global roles by name.
        """
        self._global_roles_by_name = None

    def get(self, role_id: int | str) -> GlobalRole:
        """Retrieve a specific global role.

        Note:  The C8Y REST API does not support direct query by name. Hence,
        searching by name will actually retrieve all available groups and
        return the matching ones.
        These groups will be cached internally for subsequent calls.

        See also method ``reset_caches``

        Args:
            role_id (int|str):  An actual global role ID as int/string or a
                global role name

        Returns:
            A GlobalRole instance for the ID/name.
        """
        try:
            # the following will fail if the ID is not int-like
            role_id = str(int(role_id))
            return GlobalRole.from_json(super()._get_object(role_id))
        except ValueError:
            if not self._global_roles_by_name:
                self._global_roles_by_name = {g.name: g for g in self.get_all()}
            return self._global_roles_by_name[role_id]

    def select(self, username: str = None, page_size: int = 5) -> Generator[GlobalRole]:
        """Iterate over global roles.

        Args:
            username (str): Retrieve global roles assigned to a specified user
                If omitted, all available global roles are returned
            page_size (int): Maximum number of entries fetched per requests;
                this is a performance setting

        Return:
            Generator of GlobalRole instances
        """
        # unfortunately, as selecting by username can't be implemented using the
        # generic _iterate method, we have to do everything manually.
        if username:
            # select by username
            query = f'/user/{self.c8y.tenant_id}/users/{username}/groups?pageSize={page_size}&currentPage='
            page_number = 1
            while True:
                response_json = self.c8y.get(query + str(page_number))
                references = response_json['references']
                if not references:
                    break
                for ref_json in references:
                    result = GlobalRole.from_json(ref_json['group'])
                    result.c8y = self.c8y  # inject c8y connection into instance
                    yield result
                page_number = page_number + 1
        else:
            # select all
            base_query = self._prepare_query(page_size=page_size)
            yield from super()._iterate(base_query, page_number=None, limit=None, parse_fun=GlobalRole.from_json)

    def get_all(self, username: str = None, page_size: int = 1000) -> List[GlobalRole]:
        """Retrieve global roles.

        Args:
            username (str): Retrieve global roles assigned to a specified user
                If omitted, all available global roles are returned
            page_size (int): Maximum number of entries fetched per requests;
                this is a performance setting

        Return:
            List of GlobalRole instances
        """
        return list(self.select(username=username, page_size=page_size))

    def assign_users(self, role_id: int | str, *usernames: str):
        """Add users to a global role.

        Args:
            role_id (int|str):  Technical ID of the global role
            *usernames (str):  Iterable of usernames to assign
        """
        path = self.build_object_path(role_id) + '/users'
        for username in usernames:
            user_reference = UserUtil.build_user_reference(self.c8y.tenant_id, username)
            self.c8y.post(path, user_reference, accept='')

    def unassign_users(self, role_id: int | str, *usernames: str):
        """Remove users from a global role.

        Args:
            role_id (int|str):  Technical ID of the global role
            *usernames (str):  Iterable of usernames to unassign
        """
        base_path = self.build_object_path(role_id) + '/users/'
        for username in usernames:
            self.c8y.delete(base_path + username)

    def assign_permissions(self, role_id: int | str, *permissions: str):
        """Add permissions to a global role.

        Args:
            role_id (int|str):  Technical ID of the global role
            *permissions (str):  Iterable of permission ID to assign
        """
        # permissions are called 'roles' in the Cumulocity data model
        path = self.build_object_path(role_id) + '/roles'
        for permission in permissions:
            reference = PermissionUtil.build_reference(permission)
            self.c8y.post(path, reference, accept='')

    def unassign_permissions(self, role_id: int | str, *permissions: str):
        """Remove permissions from a global role.

        Args:
            role_id (int|str):  Technical ID of the global role
            *permissions (str):  Iterable of permission ID to assign
        """
        # permissions are called 'roles' in the Cumulocity data model
        base_path = self.build_object_path(role_id) + '/roles/'
        for permission in permissions:
            self.c8y.delete(base_path + permission)
