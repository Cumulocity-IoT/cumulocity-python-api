The **Cumulocity Python API**'s measurements API (see also classes
`Measurements` and
`Measurement`) includes the following
additions to allow easy creation of standard measurement values
including units.

Effectively, each of the value classes represent a value fragment, e.g.
`Celsius`:

``` json
{"unit": "°C", "value": 22.8}
```

These values can easily be combined, e.g. when constructing a
measurement:

``` python
m = Measurement(
    type='cx_LevelMeasurement', source=device_id, time='now',
    cx_Levels={
        'oil': Liters(8.4),
        'gas': Liters(223.18),
        'h2o': Liters(1.2),
        'bat': Percentage(85)
    })
```