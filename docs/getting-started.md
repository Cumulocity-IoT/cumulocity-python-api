# Getting started with the Cumulocity Python API

In this article, we want to give you a step-by-step introduction to the
possibilities of the Cumulocity Python API, the `c8y_api` module. We
will create a simple application, read device metadata, and create and
investigate alarms.

## Preliminaries

If you want to follow this guide you'd need access to a Cumulocity IoT
tenant (duh!). If you don't have one, yet, register for your free trial
[here](https://www.cumulocity.com/start-your-journey/free-trial/) and register your mobile phone as a device following [this
guide](https://cumulocity.com/guides/users-guide/sensor-app/#overview).

It's done in no time!

I also assume that you have some basic understanding of the Python
programming language, running Python applications and the Pip package
manager.

Also, the Cumulocity Python API is closely aligned to the Cumulocity
REST API. So, a fair understanding of RESTful API is definitely helpful,
but not a requirement.

## A brief introduction into the Cumulocity Python API

The Cumulocity Python API library aims to be the standard Python library
for Cumulocity IoT application development. It is intended to be used
for standalone applications and microservices.

The API was designed with ease-of-use and performance in mind:

- It is *pythonic*, using standard Python data types, libraries,
  conventions and idioms. Python developers will find their way in a
  breeze.
- It is fully documented including all function parameters which enables
  full code completion in the programming IDE of your choice.
- It encapsulates all Cumulocity IoT low level connectivity, takes care
  of authentication, session handling and payload parsing/formatting.
- It automatically applies common performance optimizations like
  pagination, lazy, on demand parsing/formatting and payload
  minimization.

For this API we decided against creating a full Python abstraction of
the Cumulocity IoT functional, data and access model. Instead, the API
is closely aligned to the concepts of the standard [Cumulocity REST
API](https://cumulocity.com/api/core/). We won't hide the data model
details, won't perform additional requests in the background, and alike.

So, while working with the Cumulocity Python API you will also
understand how to work with the standard REST API. You will always know
exactly what's going on. You will always be able to perform a direct
REST query for edge cases - the API provides a set of nice access points
just for that.

The Cumulocity Python API is not an official API provisioned by the
Cumulocity GmbH. It's an open source project ([hosted on
GitHub](https://github.com/Cumulocity-IoT/cumulocity-python-api)),
maintained by the Cumulocity GmbH consultants and other Cumulocity
experts world wide. You are invited to contribute!

## Getting started - project setup

We want to create an application which connects to a Cumulocity IoT
tenant and lists the connected devices with owner details. The API was
designed with Docker on Cumulocity in mind, but creating a stand-alone
application is just as easy.

We will call the application/project `firstApp` - feel free to choose
your own catchy name and be sure to change the listings below
accordingly.

``` shell
$ mkdir -p firstApp/src
$ cd firstApp
```

## Create a virtual environment and get the c8y-API library

We recommend using a virtual environment by default, you can do without
but there is no reason not to. If you are new to Python and virtual
environments have a look here: [Python Virtual Environments: A
Primer](https://realpython.com/python-virtual-environments-a-primer/#how-can-you-work-with-a-python-virtual-environment)

``` shell
$ python3 -m venv venv            # create virtual environment
$ source venv/bin/activate        # step into the virtual environment
(venv) pip3 install c8y-api       # install the Cumulocity Python API
```

The last command will download the latest version of the Cumulocity
Python API (the module is abbreviated `c8y-api`) including all
dependencies from [pypi.org](http://pypi.org) and install it into the
previously created virtual environment (indicated by the `(venv)`
prompt).

Now we are all set to start developing!

## Connecting to the Cumulocity tenant

We will start by creating our main Python script, e.g. `src/app.py` in
a code editor of your choice. For this guide we won't need anything
more sophisticated.

Our entry point to the world of Cumulocity IoT is through the
`CumulocityApi` class which can be imported from the `c8y_api` library
using

``` python
from c8y_api import CumulocityApi

c8y = CumulocityApi(
      base_url='',    # the url of your Cumulocity tenant here
      tenant_id='',   # the tenant ID of your Cumulocity tenant here
      username='',    # your Cumulocity IoT username
      password='',    # your Cumulocity IoT password
   )
```

The `CumulocityApi` class can be initialized with all necessary
connection and authentication details for your Cumulocity IoT tenant.
Don't worry! There are more advanced, enterprise-ready methods to
provide this information, but for now this one is a lot more explicit
and easier to use.

## First action!

In this first application we will simply iterate through all registered
devices and list their Cumulocity object ID, designation and owner:

``` python
for d in c8y.device_inventory.select():
    print(f"Found device #{d.id} '{d.name}', owned by {d.owner}")
```

Let's have a look at this in detail. You can see that access to the
device inventory is provided through the `device_inventory` property of
the `CumulocityApi` class. Likewise, it provides access to events,
alarms, managed objects and all other aspects of the Cumulocity
information model. Feel free to explore!

Looping through objects is provided through the `select` function. This
function features many parameters, primarily to specify selection
filters. We don't worry about these right now because we simply want to
see everything. Internally, this function sends a `GET` request to the
`/inventory/managedObjects` endpoint, parses the result, and produces
corresponding Python objects.

The return of the `select` function is a series of `Device` instances
that you can work with directly. In this example we simply print the
Cumulocity IoT object ID, the device name and the device owner. All
given properties of a `Device` object in Cumulocity are represented as
corresponding class properties in Python. And - as the result of the
`select` function is typed - code completion works as well.

This is it! Assuming that you are in the project base folder and you've
put your code into file `src/app.py` you can run your first application
by

``` shell
(venv) python src/app.py
```

This outputs the metadata of all registered devices onto the console.

## Creating an alarm

Now, we will start changing things. If you already have some experience
with Cumulocity IoT you might know about its flexible information model.
We will make use of it by creating an alarm with custom fragments.

An alarm can be created by creating an `Alarm` object and posting it.
The `Alarm` class can be imported from the `c8y_api.model` package. We
also import the standard `datetime` class to time the alarm properly:

``` python
from c8y_api.model import Alarm
from datetime import datetime, timezone
```

To raise an alarm for a specific device, we need the Cumulocity IoT
object ID of one of the registered devices. Luckily, we just printed all
of them in the previous section. You might just pick one of them by
updating this line:

``` python
device_id = '' # your device ID needs to be inserted here
```

You can also just pick ID of the last device listed before like this:

``` python
device_id = d.id  # d is still in memory from the loop
```

The `Alarm` class' constructor features named parameters for the alarm's
core properties like `type` and `time`. Please note that we specify the
device by pushing the previously copied Cumulocity object ID into the
`source` parameter.

``` python
alarm_time = datetime.now(timezone.utc)

test_alarm = Alarm(
      type='cx_TestAlarm',
      time=alarm_time,
      source=device_id,
      text=f"Test alarm at {alarm_time}",
      severity=Alarm.Severity.WARNING)

c8y.alarms.create(test_alarm)
```

After instantiation, the object is then inserted into Cumulocity IoT
using the `create` function which is one of many held at the `alarms`
property of the `CumulocityApi` instance we previously set up.

Go ahead an run our changes. you won't see any additional output, but
you should now be able to locate the created alarm within the Cumulocity
IoT web interface.

## Custom fragments

Let's extend this scenario a bit. As previously said, Cumulocity IoT
features a very flexible information model - virtually any JSON
structure can be attached to any database object as custom fragments
(see also: [Cumulocity IoT's domain model](https://cumulocity.com/guides/concepts/domain-model/#fragments)).

Likewise, we can simply provide additional properties as custom
fragments after the standard parameters in the Cumulocity Python API:

``` python
test_alarm = Alarm(
      type='cx_TestAlarm',
      time=alarm_time,
      source=device_id,
      text=f"Test alarm at {alarm_time}",
      severity=Alarm.Severity.WARNING,
      # custom fragments below
      cx_CustomData={'foo': 'bar', 'data': {'is_important': True}})
```

Here, we added a fragment named `cx_CustomData` with some random data in
it. As you can see, you can provide any JSON structure here.

Alternatively you can add such fragments after object instantiation
using the `[]` operator:

``` python
test_alarm['cx_MoreData'] = {'nice': True}
```

Once these fragments are present, you can easily access them using
standard Python notation:

``` python
test_alarm['cx_CustomData']['foo']          # access using [] notation
test_alarm.cx_CustomData.data.is_important  # access using dot notation
```

There is also the possibility to address JSON fields "by path":

``` python
test_alarm.get('cx_CustomData.data.is_important')  # will return None if not existing
test_alarm.get('cx_CustomData.data.is_important', False)  # will return default (False) if not existing
```

Let's loop through all alarms and list their details:

``` python
for a in c8y.alarms.select(source=device_id):
    print(f"Found alarm #{a.id}, {a.text}, fragments: {list(a.keys())}")
    if 'cx_CustomData' in a:
        print(f"   Important: {a.cx_CustomData.data.is_important}")
        print(f"   More data: {a['cx_CustomData']['foo']}")
```

Like before, when we looped through the devices, we use a `select`
function to loop through objects. Note that we are working with the
`alarms` instead of the `device_inventory` resource this time. The
Cumulocity Python API defines multiple of these resources that all
behave in the same way.

You can see a lot of additional features of the API as well. First of
all, we introduced a filter: we only select alarms that are assigned to
our device using the `source` parameter for filtering. When exploring
`Alarm` objects we can work with fragments using standard Python
notation: We use the `keys()` function to list custom fragments, the
`in` operator to check for specific fragments and the `[]` operator as
well as *dot notation* to address specific properties of these
fragments.

You can run this application again. You will see additional output that
lists all alarms (the just created and any previous ones), including the
custom fragments.

## Clearing alarms

Within Cumulocity IoT, alarms are special events that need manual
intervention. They feature a lifecycle and correspondingly can only be
created once (creating an identical alarm multiple times does not raise
the alarm again, see also [Cumulocity IoT's Event model](https://cumulocity.com/guides/concepts/domain-model/#events).

Because of this, we can run our sample application multiple times
without spamming the platform with additional alarms. A alarm can only
be raised (i.e. created) again, when it was previously acknowledged and
cleared. We can do this in the UI (feel free to do that right now!) or
we can do this using Python.

Updating via the Cumulocity Python API is particularly easy. Let ups
loop through all alarms of our device and clear them:

``` python
for a in c8y.alarms.select(source=device_id, status=Alarm.Status.ACTIVE):
    a.status = Alarm.Status.CLEARED
    a.update()
    print(f"Alarm #{a.id} cleared.")
```

Like before we use the `select` function to loop over the alarms. This
time, we add another filter for the alarm's status - no need to visit
inactive alarms.

To update an alarm we simply update the status property of the instance
and invoke the `update` function. Internally this will create a POST
request which will push the changes (the status update) to Cumulocity
IoT.

Invoking the `update` function directly on the Alarm instance is what we
call the **object-oriented invocation style**. In fact, if you prefer
differently you can also invoke the update function **functional style**
on the `CumulocityApi` instance with the same result.

``` python
c8y.alarms.update(a)   # this would work as well
```

You can now run the application over and over again. It will

> - first list all devices,
> - then create an alarm for one of them,
> - list all already created alarms
> - acknowledge all open alarms

## A note to pro-users

You might think that you could have updated our alarm directly without
the loop like this:

``` python
test_alarm.status = Alarm.Status.CLEARED
test_alarm.update()   # this does not work
```

This won't work. Why? Well, in the end the `update` function of the
`Alarm` class needs to send a POST request towards Cumulocity IoT. To be
able to do that it needs to have access to a valid connection. We
haven't specified that. Also, to update an object you need the
Cumulocity IoT object ID (of the alarm). We haven't specified that
either.

If you know the ID of the alarm object, you could do that, though:

``` python
test_alarm.c8y = c8y   # specify cumulocity connection
test_alarm.id  = ''    # specify the alarm object id

test_alarm.status = Alarm.Status.CLEARED
test_alarm.update()    # this would work now
```

Ok, but wait! Why does the very same then work within the loop? Well,
because both - the connection reference and the object ID - are injected
into the `Alarm` instances generated by the `select` function
automatically. Neat, right?

## Where to next?

Hopefully you had fun following this quick start guide and you got
interested in learning more. Please feel free to experiment! We hope
that we were able to show that the Cumulocity Python API makes
development for Cumulocity IoT as easy as it can possibly be.

Some hints where to go next:

- Build your own metadata using the Cumulocity inventory. The Cumulocity
  Python API makes handling custom fragments particularly easy!

- Have a look at measurements! You can use the API to easily grab
  measurements of a specific types, timeframes and other
  characteristics. You can also create measurements using a neat
  object-oriented API

If you are interested in participating in the further development of the
Cumulocity Python API, please join our [GitHub community](https://github.com/Cumulocity-IoT/cumulocity-python-api).
