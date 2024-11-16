# Copyright (c) 2020 Software AG,
# Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
# and/or its subsidiaries and/or its affiliates and/or their licensors.
# Use, reproduction, transfer, publication or disclosure is prohibited except
# as specifically provided for in your License Agreement with Software AG.

from c8y_api import CumulocityApi

import pytest


def test_select_name(live_c8y: CumulocityApi):
    """Verify that select by name works."""
    apps = live_c8y.applications.get_all(name='devicemanagement')
    assert apps
    app = apps[0]
    assert app.name == 'devicemanagement'
    assert app.owner == 'management'
    assert app.type == 'HOSTED'
    assert app.availability == 'MARKET'


def test_select_owner(live_c8y: CumulocityApi):
    """Verify that select by owner works."""
    # this test assumes, that the live tenant owns at least one application
    apps = live_c8y.applications.get_all(owner=live_c8y.tenant_id)
    assert apps


@pytest.mark.parametrize('param, param_func', [
    ('type', lambda x: 'HOSTED'),
    ('user', lambda x: x.username),
    ('tenant', lambda x: x.tenant_id),
    ('subscriber', lambda x: x.tenant_id),
    ('provided_for', lambda x: x.tenant_id),
])
def test_selects(live_c8y: CumulocityApi, param, param_func):
    """Verify that select/get_all works with all available filters."""
    kwargs = {param: param_func(live_c8y)}
    apps = live_c8y.applications.get_all(**kwargs)
    assert apps


@pytest.fixture(name='bootstrap_api', scope='module')
def fix_bootstrap_api(app_factory):
    """Provide a CumulocityApi instance with bootstrap permissions."""
    app_name = 'inttest-application'
    required_roles = ['ROLE_OPTION_MANAGEMENT_READ', 'ROLE_OPTION_MANAGEMENT_ADMIN']
    return app_factory(app_name, required_roles)


def test_get_current(bootstrap_api):
    """Verify that the current application can be read using
    a bootstrap instance."""
    app = bootstrap_api.applications.get_current()
    # the format of the username is "boostrapuser_<appname>"
    bootstrap_app_name = bootstrap_api.username.split('_', 1)[1]
    assert app.name == bootstrap_app_name


def test_get_current_settings(live_c8y, bootstrap_api):
    """Verify that the current application's settings can be read using
    a bootstrap instance."""
    assert bootstrap_api.applications.get_current_settings() is not None


def test_get_current_subscriptions(live_c8y, bootstrap_api):
    """Verify that the current application's subscriptions can be read using
    a bootstrap instance."""
    subscriptions = bootstrap_api.applications.get_current_subscriptions()
    assert len(subscriptions) == 1
    assert subscriptions[0].tenant_id == live_c8y.tenant_id
