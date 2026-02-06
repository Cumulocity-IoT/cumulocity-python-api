The **Cumulocity Python API** (`c8y_api` module) provides a convenience
wrapper around the standard Cumulocity REST API (see also the [OpenAPI
documentation](https://cumulocity.com/api/core/)).

The [CumulocityRestApi][c8y_api.CumulocityRestApi] class provides the
fundamental wrapping around authentication and basic `get`, `post`, 
`put`, `delete` commands. The [CumulocityApi][c8y_api.CumulocityApi]
class is your entrypoint into higher level funct    ions, grouped by
contexts like `inventory`, `users`, and `measurements`. Each of these
contexts is documented in detail within the
`main-api-classes` section.

The [CumulocityDeviceRegistry][c8y_api.CumulocityDeviceRegistry] class
provides an additional entry point for devices, wrapping the entire
bootstrap mechanism. See also the [Device integration
documentation](https://cumulocity.com/guides/device-sdk/rest/#device-integration)
at Cumulocity.
