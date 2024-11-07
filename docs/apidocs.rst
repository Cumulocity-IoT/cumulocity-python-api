.. Copyright (c) 2020 Software AG,
   Darmstadt, Germany and/or Software AG USA Inc., Reston, VA, USA,
   and/or its subsidiaries and/or its affiliates and/or their licensors.
   Use, reproduction, transfer, publication or disclosure is prohibited except
   as specifically provided for in your License Agreement with Software AG.

API Documentation
=====================

REST API Connector
------------------

The **Cumulocity Python API** (``c8y_api`` module) provides a convenience wrapper
around the standard Cumulocity REST API (see also the
`OpenAPI documentation <https://cumulocity.com/api/core/>`_).

The :class:`CumulocityRestApi <c8y_api.CumulocityRestApi>` class provides the fundamental wrapping around
authentication and basic ``get``, ``post``, ``put``, ``delete`` commands.
The :class:`CumulocityApi <c8y_api.CumulocityApi>` class is your entrypoint into higher level functions,
grouped by contexts like ``inventory``, ``users``, and ``measurements``.
Each of these contexts is documented in detail within the
:ref:`main-api-classes` section.

The :class:`CumulocityDeviceRegistry <c8y_api.CumulocityDeviceRegistry>` class provides an additional entry point
for devices, wrapping the entire bootstrap mechanism. See also the
`Device integration documentation <https://cumulocity.com/guides/device-sdk/rest/#device-integration>`_.

.. automodule::  c8y_api
    :members: CumulocityRestApi, CumulocityApi, CumulocityDeviceRegistry
    :special-members: __init__


Application Helpers
-------------------

The **Cumulocity Python API** (``c8y_api`` module) is designed to be
particularly useful for developing Cumulocity microservices. For this,
the module provides two helper classes that take care of microservice
specific authentication.

The :class:`SimpleCumulocityApp <c8y_api.app.SimpleCumulocityApp>` class should be used for single tenant
microservices. It automatically reads the microservice's environment
to determines the microservice access credentials.

The :class:`MultiTenantCumulocityApp <c8y_api.app.MultiTenantCumulocityApp>` class should be used for multi-tenant
microservices which need to handle requests for arbitrary Cumulocity
tenants. It reads the microservice's environment to determine the
necessary bootstrap credentials and provides additional functions to
dynamically obtain :class:`CumulocityApi` instances for specific tenants.

.. automodule::  c8y_api.app
    :members: SimpleCumulocityApp, MultiTenantCumulocityApp
    :inherited-members:
    :special-members: __init__

.. _main-api-classes:

Main API Classes
----------------

The **Cumulocity Python API**'s main API classes provide access to
various contexts within the Cumulocity REST API. Use it to read
existing data or modify in bulk.

See also the :ref:`object-models` section for object creation and
object-oriented access in general.

.. automodule:: c8y_api.model
    :members: Inventory, DeviceInventory, Identity, Binaries, Measurements, Events, Alarms, Users, GlobalRoles, InventoryRoles, Subscriptions, Tokens, Operations, BulkOperations, Applications, TenantOptions, AuditRecords

.. _object-models:

Object Models
-------------

The **Cumulocity Python API**'s object model provides object-oriented
access to the Cumulocity REST API. Use it to create and modify single
objects within the Database.

These objects can also be used directly within the :ref:`main-api-classes`
to modify data in bulk.

.. automodule:: c8y_api.model
    :members: ManagedObject, Device, DeviceGroup, ExternalId, Binary, Measurement, Event, Alarm, Series, Subscription, Availability, Fragment, NamedObject, User, CurrentUser, TfaSettings, GlobalRole, Permission, ReadPermission, WritePermission, AnyPermission, Operation, BulkOperation, Application, ApplicationSetting, ApplicationSubscription, TenantOption, AuditRecord, Change ,
    :special-members: __init__
    :undoc-members:


Measurement Additions
----------------------

The **Cumulocity Python API**'s measurements API (see also classes
:class:`Measurements` and :class:`Measurement`) includes the following additions
to allow easy creation of standard measurement values including units.

Effectively, each of the value classes represent a value fragment, e.g. ``Celsius``:

.. code-block:: json

   {"unit": "°C", "value": 22.8}

These values can easily be combined, e.g. when constructing a measurement:

.. code-block:: python

   m = Measurement(type='cx_LevelMeasurement', source=device_id, time='now',
                   cx_Levels={
                       'oil': Liters(8.4),
                       'gas': Liters(223.18),
                       'h2o': Liters(1.2),
                       'bat': Percentage(85)
                   })

.. automodule:: c8y_api.model
    :members: Units, Celsius, Centimeters, Count, CubicMeters, Grams, Kelvin, Kilograms, Liters, Meters, Percentage, Value
    :undoc-members:


.. _auxiliaries:

Auxiliary Tools
----------------

.. automodule:: c8y_tk
    :special-members: __init__
    :undoc-members:
