# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

# pylint: disable=redefined-outer-name

import secrets
import string
import time
from contextlib import suppress
from typing import Union

import pytest
import pyotp

from c8y_api import CumulocityApi
from c8y_api.model import User

from util.testing_util import RandomNameGenerator


def generate_password():
    """Generate a strong password meeting Cumulocity requirements."""
    alphabet = string.ascii_letters + string.digits + '_-.#&$'
    return 'Aa0.' + ''.join(secrets.choice(alphabet) for _ in range(12))


def test_CRUD(live_c8y: CumulocityApi):  # noqa (case)
    """Verify that basic CRUD functionality works."""

    username = RandomNameGenerator.random_name()
    email = f'{username}@software.ag'

    user = User(c8y=live_c8y,
                username=username, email=email,
                enabled=True)

    created_user = user.create()
    try:
        assert created_user.id == username
        assert created_user.password_strength == 'GREEN'
        assert created_user.require_password_reset
        assert created_user.tfa_enabled is False

        created_user.require_password_reset = False
        created_user.last_name = 'last_name'
        updated_user = created_user.update()

        assert updated_user.last_name == created_user.last_name
        assert updated_user.require_password_reset == created_user.require_password_reset
    finally:
        created_user.delete()

    with pytest.raises(KeyError) as e:
        live_c8y.users.get(user.username)
        assert user.username in str(e)


def test_select_by_name(live_c8y: CumulocityApi):
    """Verify that user selection by name works as expected."""
    prefix = RandomNameGenerator.random_name(1)
    users = []
    try:
        for _ in range(0, 5):
            username = f'{prefix}-{RandomNameGenerator.random_name(1)}'
            email = f'{username}@c8y.com'

            user = User(live_c8y, username=username, email=email, enabled=True).create()
            users.append(user)

        selected = live_c8y.users.get_all(username=prefix)
        assert {x.id for x in selected} == {x.id for x in users}
    finally:
        for u in users:
            with suppress(Exception):
                u.delete()


def test_get_current(live_c8y: CumulocityApi):
    """Verify that the current user can be read."""
    current1 = live_c8y.users.get(live_c8y.username)
    current2 = live_c8y.users.get_current()

    assert current1.username == current2.username
    assert current1.id == current2.id

    assert all(i in current2.effective_permission_ids for i in current1.permission_ids)


def test_current_update(live_c8y: CumulocityApi, user_c8y: CumulocityApi):
    """Verify that updating the current user works as expected."""
    current_user = user_c8y.users.get_current()

    current_user.first_name = "New"
    current_user = current_user.update()
    assert current_user.first_name == "New"


def test_current_totp(live_c8y: CumulocityApi, user_c8y: CumulocityApi):
    """Verify that the TOTP settings can be updated for the current user."""
    current_user = user_c8y.users.get_current()

    # a new user without TFA won't have the TOTP activity set up
    with pytest.raises(KeyError):
        current_user.get_totp_activity()

    # the auxiliary function should intercept the KeyError
    assert not current_user.get_totp_enabled()

    # generating a secret won't enable TOTP
    secret, _ = current_user.generate_totp_secret()
    assert not current_user.get_totp_activity().is_active

    # explicitly enabling the feature using different methods
    current_user.enable_totp()
    assert current_user.get_totp_enabled()
    assert current_user.get_totp_activity().is_active

    # generate and verify TOTP codes
    totp = pyotp.TOTP(secret)
    code = totp.now()
    current_user.verify_totp(code)

    # wait for the code to become invalid
    while code == totp.now():
        time.sleep(1)
    # Cumulocity has a tolerance for the last code
    time.sleep(30)

    assert not current_user.is_valid_totp(code)
    with pytest.raises(ValueError) as ex:
        current_user.verify_totp(code)
    assert '403' in str(ex)
    assert 'Invalid verification code' in str(ex)

    # Simply disabling the TOTP feature is no longer supported (v10.20)
    with pytest.raises(ValueError) as ex:
        current_user.disable_totp()
    assert '403' in str(ex)
    assert 'Cannot deactivate TOTP setup!' in str(ex)

    # Revoking does automatically disable the feature
    live_c8y.users.revoke_totp_secret(current_user.username)
    assert not current_user.get_totp_enabled()
    assert not current_user.get_totp_activity().is_active


@pytest.fixture(scope='function')
def user_factory(live_c8y: CumulocityApi):
    """Provides a user factory function which removes the created users after
    the test execution."""

    created_users = []

    def factory_fun(with_password=False) -> Union[User, tuple[User, str]]:
        username = RandomNameGenerator.random_name(2)
        email = f'{username}@software.ag'
        password = generate_password()
        print(f"User: {email}, Password: {password}")
        user = User(c8y=live_c8y, username=username, password=password, email=email).create()
        created_users.append(user)
        if with_password:
            return user, password
        return user

    yield factory_fun

    for u in created_users:
        u.delete()


@pytest.fixture(scope='function')
def user_c8y(live_c8y: CumulocityApi, user_factory):
    """Provides a Cumulocity connection for a new user."""
    new_user, password = user_factory(with_password=True)

    return CumulocityApi(base_url=live_c8y.base_url, tenant_id=live_c8y.tenant_id,
                         username=new_user.username, password=password)


def test_current_set_password(live_c8y: CumulocityApi, user_c8y):
    """Verify that the password of a user can not be set."""

    user = user_c8y.users.get_current()

    # password strength requirements are tested before updating
    with pytest.raises(ValueError) as ve:
        user.update_password(user_c8y.auth.password, 'pw')
        assert 'least' in str(ve)

    # store last password change datetime
    before_datetime = user.last_password_change_datetime

    # updating for the current user should be ok
    new_password = generate_password()
    user.update_password(user_c8y.auth.password, new_password)

    # password timestamp should have been updated
    user = user_c8y.users.get_current()
    assert user.last_password_change_datetime != before_datetime

    # follow-up requests should still work
    assert len(user_c8y.inventory.get_all(limit=10)) == 10


def test_set_owner(live_c8y: CumulocityApi, user_factory):
    """Verify that the owner of a user can be set and removed."""

    user1 = user_factory()
    user2 = user_factory()

    # 1) set an owner using the OO method
    user1.set_owner(user2.username)
    db_user1 = live_c8y.users.get(user1.username)
    # -> owner property must be set to owner ID
    assert db_user1.owner == user2.username

    # 2) delete/unset an owner using the resource function
    live_c8y.users.set_owner(user1.username, None)
    db_user1 = live_c8y.users.get(user1.username)
    # -> owner property must be unset
    assert not db_user1.owner


def test_set_delegate(live_c8y: CumulocityApi, user_factory):
    """Verify that the delegate of a user can be set and removed."""

    user1 = user_factory()
    user2 = user_factory()

    # 1) set the delegate using the OO method
    user1.set_delegate(user2.username)
    db_user1 = live_c8y.users.get(user1.username)
    # -> owner property must be set to owner ID
    assert db_user1.delegated_by == user2.username

    # 2) delete/unset an owner using the resource function
    live_c8y.users.set_delegate(user1.username, None)
    db_user1 = live_c8y.users.get(user1.username)
    # -> owner property must be unset
    assert not db_user1.delegated_by


def test_get_tfa_settings(live_c8y, user_c8y):
    """Verify that the TFA settings can be retrieved as expected."""

    # all users have TFA settings
    tfa_settings = live_c8y.users.get_tfa_settings(user_c8y.username)
    assert tfa_settings
    assert not tfa_settings.enabled

    # to enable TFA, we first generate a secret, then enable
    # this is only possible for the current user
    current_user = user_c8y.users.get_current()
    current_user.generate_totp_secret()
    current_user.enable_totp()
    assert live_c8y.users.get_tfa_settings(current_user.username).enabled
