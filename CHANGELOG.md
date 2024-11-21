# Changelog

## Version 3

* Unified query behaviour of all API classes (introducing potentially breaking changes as the order of parameters needed to change).
* Added support for arbitrary expressions and kwargs on almost all select functions defined at the API classes, e.g. for Measurements, Events, Alarms, Inventory, etc. Using the expression parameter (always the first in all select-like functions that support it), an arbitrary query expression can be defined which will be forwarded to the REST API as-is. Using kwargs, additional maybe undocumented or deprecated parameters can be defined.
* Additional, undocumented select parameters will automatically be converted to Pascal case (e.g. my_undocumented_arg will be translated to myUndocumentedArg).
* Incorporated pull request to remove dependency on deprecated pkg_resources package (thanks @reubenmiller).
* Incorporated pull request to support context handlers.
* Many additional unit tests and integration tests.
* Fixed issue [#63](https://github.com/Cumulocity-IoT/cumulocity-python-api/issues/63) (tenant option select function did not filter categories correctly).


## Version 2.1

* Added support for processing mode on all API base classes
* Added support for Cookie-based auth on OAI-only tenants
* Added latest extensions for Notification 2.0 API including `count` function.
* Switch to Python version 3.10


## Version 2.0

* Added Changes support to the Audit API.
* Fixed Issue #53 "KeyError when retrieving 'bulkOperation'"; bulk operations JSON is somewhat _non-standard_ as 
  the root element is not named like the corresponding REST resource.
* Added proper support for the CurrentUser API; this is a breaking change as some functions moved from the User API
  to the CurrentUser API (the correct place).
* Added support for 2FA at user level; TFA/TOTP can be enabled for individual users. Parts of this functionality,
  e.g. getting the TOTP secret are only available at the CurrentUser level
* Adding traditional date filter parameter names (date_from and date_to in addition to before/after) to Events 
  and Alarms API. 


## Version 1.10

* The `select` and `get_all` functions now feature an `expression` parameter which allows to directly specify the entire REST API filtering expression.
* Fixed, unified and streamlined the behavior or the `query` parameter within all `select` and `get_all` functions.
* The `apply_to` functions now allow to specify the to-be-applied changes directly in JSON. 
* Various tiny code and documentation improvements.
* Updated GitHub Actions to latest Node versions.
* Fixed build dependencies.


## Version 1.9.2

* Testing code improvements.
* Added support for signed, shared and non-persistent Notification 2.0 subscriptions and tokens (Thanks @wilbersl!)
* Fixed audit record parsing.
* Various code and documentation improvements.
* Added support for token-based authentication for interactive sessions.
* Added page_number parameter to inventory queries to be able to pull a specific page.
* Added get_count functions to inventory to estimate expected number of results.
* Added get_subscribers function MultiTenantCumulocityApp.


## Version 1.9.1

* Minor improvements and fixes.
* Added possibility to pull a specific result page to all `select` and `get_all` functions.


## Version 1.9.0

* Added support for inventory endpoints `/availability`, `/supportedMeasurements` and `/supportedSeries`.

* Added `Units` class to support explicit modelling of measurement fragments.

* Added support for the Current Application API (current application settings, current application subscriptions).

* Added test fixture (`app_factory`)to `conftest.py` to register (and automatically unregister) a dedicated
  microservice application for advanced integration testing. 

* Making websocket ping interval explicit and updating it to 60 seconds by default.


## Version 1.8.2

* Bumped flask from 2.2.2 to 2.3.2 (vulnerability)

  Bumped python-dateutil from 2.8.1 to 2.8.2 (pandas requirement)

* Added `is_tls` property to `CumulocityRestApi` class;

  fixed secure protocol handling for Notification2 websocket connections.

* Microservice build support improvements.


## Version 1.8.1

* Fixed series value collection for incomplete series.


## Version 1.8

* Adding support for measurement series queries.

## Version 1.7

* Adding support for the Audit API.

* Added support for event attachment handling.

* Adding support for bulk operations.

## Version 1.6.1

* Adding `c8y_tk` namespace to distribution.
 
## Version 1.6

* Added API support for Notification 2.0 subscriptions and tokens.
 
* Added new package c8y_tk for additional features.
 
* Added synchronous and asynchronous Notification 2.0 websocket listener,
  Added two (async/sync) Notification 2.0 samples.

## Version 1.5

* Improved Applications API.

* Added microservice utilities for easier testing of provided samples.

* Added Tenant Options API support.

## Version 1.4

* Fixed https://github.com/SoftwareAG/cumulocity-python-api/issues/25
  The SimpleTenantApp did not include the tenant ID into the username which is not supported 
  by all Cumulocity instances.

* Adding class _QueryUtil, bundling query encoding related functionality.

* Added tests for special character parsing.

* Fixed handling and documentation of inventory API for querying by name. 
  Added query parameter for specification of custom queries.

* Reverted changes in ComplexObject - a ComplexObject is not a dictionary-like class, it only   
  supports some dictionary-like access functions. But, for instance, updating a ComplexObject
  is very different from updating a dictionary. Hence, it no longer inherits MutableMapping.

## Version 1.3.2

### Changed

* Obfuscated internal properties in _DictWrapper which blocked standard dictionary behavior. 
  Code cleanup.

* ComplexObject & _DictMapping now both inherit MutableMapping (Thanks Sam!).

* The base API now ignores trailing slashes gracefully.


## Version 1.3.1

### Changed

* Switched to version 2.4.0 of PyJWT as recommended by https://nvd.nist.gov/vuln/detail/CVE-2022-29217 


## Version 1.3

### Changed

* All objects with fragments can now be converted to Pandas Series (Thanks Sam!).

### Added 

* Added support for operations (Thanks Alex!).

* Added support for lastUpdated field in alarms and events.


## Version 1.2

### Changed

* Changed behavior of Events and Alarms API. Previously, an undefined event/alarm time was set to the current datetime 
  when invoking the `.create` function on the object. This was handy but inconsistent to the REST API behavior and
  therefore removed. Instead, the constructor can now be invoked with `time='now'` as a shorthand. The `time` field
  is never set to a default value automatically.

* Added `samples` folder to linting task.

* Added device agent registration sample (Thanks Nick!).


## Version 1.1.1

### Added

* Added Multi-Tenant sample script (`samples/multi_tenant_app.py`).

* Added task `build-ms` task and corresponding script files to generate Cumulocity microservices from sample scripts.

### Fixes

* Fixed authentication (username must include tenant ID) for subscribed tenants in multi-tenant scenarios. 

* Fixed pylint dependency in `requirements.txt`.

* Added `cachetools` to library dependencies in `setup.cfg`.


## Version 1.1

### Notes

* _Warning_, this release is a breaking change as it introduces an `auth` parameter to the API base classes,
  `CumulocityRestAPI` and `CumulocityAPI`. This parameter should be the new standard to use (instead of just
  username and password).

* _Warning_, this release replaces the 'all-purpose' class `CumulocityApp` with specialized versions for multi-tenant
  (`MultiTenantCumulocityApp`) and single tenant (`SimpleCumulocityApp`) environments.

### Added

* Added `_util.py` file to hold all cross-class auxiliary functionality.

* Added `_auth.py` file to hold all cross-class authentication functionality. Moved corresponding code from file
  `app.__init__.py` to the `AuthUtil` class.
 
* Added `_jwt.py` with `JWT` class which encapsulates JWT handling for the libraries purpose. This is _not_ a full
  JWT implementation. 

* Added `HTMLBearerAuth` class which encapsulates Cumulocity's JWT token-based authentication mechanism. 
 
* Added token-based authentication support. All API classes now can be initialized with an AuthBase parameter which
  would allow all kinds of authentication mechanisms. As of now, `HTTPbasicAuth` and `HTTPBearerAuth` is supported.

* Added caching with TTL/Max Size strategies to `MultiTenantCumulocityApp` and `SimpleCumulocityApp`.

* Added samples: `user_sessions.py` illustrating how user sessions can be obtained and `simple_tenant_app.py` 
  illustrating how the `SimpleCumulocityApp` class is used.

* Added requirements: `cachetools` (for caching), `inputtimeout` and `flask` (for samples).

### Changed

* Fixed file opening in `post_file` function in `_base_api.py` to avoid files already being closed when posting.

* Removed class `CumulocityApp` as it was too generic and hard to use. Replaces with classes `SimpleCumulocityApp`
  which behaves pretty much identical and `MultiTenantCumulocityApp` which behances more like a factory.


## Version 1.0.2

### Changed

* Added this changelog :-)

* Fixed [Issue #7](https://github.com/SoftwareAG/cumulocity-python-api/issues/7):
  Improved caching and user experience when creating CumulocityApp instances. Added unit tests.

* Added possibility to resolve the tenant ID from authorization headers (both `Basic` and `Bearer`).


## Version 1.0.1

### Changed

* The cumulocity-pyton-api library is now available on [PyPI](https://pypi.org) under the name `c8y_api` (see https://pypi.org/project/c8y-api/) 
* Updated README to reflect installation from PyPI 


## Version 1.0

Major refactoring of beta version:
* Unified user experience
* Complete documentation
* Performance improvements
* Introduced `CumulocityApp` to avoid mix-up with `CumulocityApi`
* Complete unit tests
* Structured integration tests
* Removed samples (sorry, need to be re-organized)
