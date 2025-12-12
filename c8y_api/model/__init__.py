# Copyright (c) 2025 Cumulocity GmbH

from c8y_api.model.administration import *
from c8y_api.model.alarms import *
from c8y_api.model.applications import *
from c8y_api.model.audit import *
from c8y_api.model.binaries import *
from c8y_api.model.events import *
from c8y_api.model.identity import *
from c8y_api.model.inventory import *
from c8y_api.model.managedobjects import *
from c8y_api.model.measurements import *
from c8y_api.model.notification2 import *
from c8y_api.model.operations import *
from c8y_api.model.tenant_options import *
from c8y_api.model.tenants import *

from c8y_api.model._base import get_by_path, as_record, as_tuple

__all__ = [
    # API Classes
    'Inventory',
    'DeviceInventory',
    'DeviceGroupInventory',
    'Binaries',
    'Identity',
    'Measurements',
    'Events',
    'Alarms',
    'Subscriptions',
    'Users',
    'GlobalRoles',
    'Operations',
    'BulkOperations',
    'Applications',
    'TenantOptions',
    'AuditRecords',
    'Tenants',
    # Model Classes
    'CumulocityResource',
    'ManagedObject',
    'Device',
    'DeviceGroup',
    'ExternalId',
    'Binary',
    'Measurement',
    'Event',
    'Alarm',
    'Series',
    'Subscription',
    'Tokens',
    'Availability',
    'Fragment',
    'NamedObject',
    'User',
    'TfaSettings',
    'CurrentUser',
    'GlobalRole',
    'InventoryRole',
    'Permission',
    'ReadPermission',
    'WritePermission',
    'AnyPermission',
    'Operation',
    'BulkOperation',
    'Application',
    'TenantOption',
    'AuditRecord',
    'Change',
    'Tenant',
    # Measurement Helpers
    'Units',
    'Celsius',
    'Centimeters',
    'Count',
    'CubicMeters',
    'Grams',
    'Kelvin',
    'Kilograms',
    'Liters',
    'Meters',
    'Percentage',
    'Value',
    # Functions
    'get_by_path',
    'as_record',
    'as_tuple',
]
