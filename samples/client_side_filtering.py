import logging

from c8y_api.app import SimpleCumulocityApp
from c8y_api.model import Device
from c8y_api.model.matcher import field, match_all, match_not, pydf
from util.testing_util import load_dotenv

logging.basicConfig(level=logging.DEBUG)

"""
This example demonstrates how to use the client-side-filtering feature of the
Python Cumulocity API. 

Client-side filtering is _not_ part of the standard API, it is an extension
which the Python API provides which _can_ be useful in _some_ use cases. It
is important to understand, that client-side-filtering is somewhat a tool of
last resort - it becomes necessary if server-side filtering is not sufficient.
Python being Python already provides nice standard features for filtering the
query results, e.g. via list comprehension or the `filter` function. However,
sometimes it may appear easier for developers to define such a filter directly
with the `select` or `get_all` function, for example in interactive scenarios.

Client-side filters are defined for the _raw_ JSON structure. Hence, when you
want to use them you must be aware of Cumulocity's JSON data format. 

By default, PyDF (Python Display Filter) expression can be specified directly
as strings. See also
https://github.com/bytebutcher/pydfql/blob/main/docs/USER_GUIDE.md#4-query-language
for details. 

Performance: Hi there, optimization kids! To put it bluntly - using this
feature does most likely _not_ increase performance in any way. This is
because the actual JSON parsing will happen in any case and this is the
expensive part. So - all the illustrated methods below do only marginally
differ in speeed.
"""

load_dotenv()  # load environment from a .env if present
c8y = SimpleCumulocityApp()
print("CumulocityApp initialized.")
print(f"{c8y.base_url}, Tenant: {c8y.tenant_id}, User:{c8y.username}")

# Let's create a couple of devices with arbitrary names
d1 = Device(c8y=c8y, type="c8y_TestDevice", name="Some Test Device").create()
d2 = Device(c8y=c8y, type="c8y_TestDevice", name="Device #2").create()
d3 = Device(c8y=c8y, type="c8y_TestDevice", name="Another Device").create()
d4 = Device(c8y=c8y, type="c8y_TestDevice", name="Machine thingy").create()

print("All devices of type 'c8y_TestDevice':")
for d in c8y.device_inventory.select(type='c8y_TestDevice'):
    print(f" - {d.name}")

# Option 1: filtering devices by name using standard Python filters
# The following select statement will simply list "all" devices (there are
# no DB filters) and subsequently filter the results using a standard Python
# list comprehension:
filtered_devices = [x for x in c8y.device_inventory.select(type='c8y_TestDevice') if 'Device' in x.name]
# -> We will only have devices which are named "Device something"
print("Option #1 result (needs to contain 'Device' string)")
for d in filtered_devices:
    print(f" - {d.name}")

# Option 2: using the client-side filtering with JMESPath filters
# The following statement will simply list "all" devices (there are no DB
# filters) and subsequently filter the results using a PyDF expression.
# The PyDF expression is matched against the unprocessed JSON, hence it is required
# to understand Cumulocity's native JSON formats (but they are close to how
# the Python API resembles it:
filtered_devices_2 = c8y.device_inventory.get_all(type='c8y_TestDevice', include="name contains Device")
# -> We will only have devices which are named "Device something"
print("Option #2 result (same thing):")
for d in filtered_devices_2:
    print(f" - {d.name}")

# Option 3: the client-side filtering with custom filters
# The following statement will simply list "all" devices (there are no DB
# filters) and subsequently filter the results using Python matchers. There
# is quite a list of predefined matchers (see c8y_api.model.matchers) and
# custom ones are easy to define.
filtered_devices_3 = c8y.device_inventory.get_all(
    type='c8y_TestDevice',
    include=field('name', '*Device*'),
    exclude=field('name', '*#*'))
# -> We will only have devices which are named "Device something" but
#    without '#' anywhere in the name
print("Option #3 result (Same, but no #)")
for d in filtered_devices_3:
    print(f" - {d.name}")

# Option 4: the client-side filtering with nested filters
# The following statement applies the same as above, but using ridiculously
# nested Python matchers to get the same logic. Note that we combine custom
# filters with a PyDF expression filter.

filtered_devices_4 = c8y.device_inventory.get_all(
    type='c8y_TestDevice',
    include=match_all(
        field('name', '*Device*'),
        match_not(
            pydf("name contains '#'")
        )
    ))
# -> Same result
print("Option #4 result (All the same)")
for d in filtered_devices_4:
    print(f" - {d.name}")


# cleanup
d1.delete()
d2.delete()
d3.delete()
d4.delete()