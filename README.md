# kairosdb-rabbitmq
KairosDB plugin for RabbitMQ. Great inspired by https://github.com/hugocore/kairosdb-rabbitmq

## Usage ##

### 1. Install ###

TODO

### 2. Configuration ###

TODO

### 3. Message formats ###

#### JSON ####

    {
      metric: "sensor.1",
      datapoints: [
      {
        tags: {"name":"flow", "unit": "m3/min"},
        values: {"1388530800000":"1.16", "1388530805000":"2.98"}
      },
      {
        tags: {"name":"pressure", "unit": "barg"},
        values: {"1388530800000":"7.10", "1388530805000":"7.05"}
      }]
    }
    
### CSV ###

Not a true CSV format, but combination of it.

    sensor.1;name:flow,unit:m3/min;1388530800000:1.16,1388530805000:2.98;name:pressure,unit:barg;1388530800000:7.10,1388530805000:7.05
    
CSV is build in format: metricname;tags1;values1;tags2;values2. Each section is divided by ;. tags and values are split by comma and represented as key:value.