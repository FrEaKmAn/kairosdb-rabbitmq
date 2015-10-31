# kairosdb-rabbitmq
KairosDB plugin for RabbitMQ. Greatly inspired by https://github.com/hugocore/kairosdb-rabbitmq. Compared to previous plugin, this plugin uses one queue to handle incoming messages. Each message has a metric attribute identifying where to save datapoints.  

## 1. Install ##

Copy the files from dist/ to the KairosDB installation folder (if installed by dpkg it's /opt/kairosdb/):

1. **/dist/conf/kairosdb-rabbitmq.properties** to **/opt/kairosdb/conf/kairosdb-rabbitmq.properties**
2. **/dist/lib/kairosdbdb-rabbitmq.jar** to **/opt/kairosdb/lib/kairosdbdb-rabbitmq.jar**
3. **/dist/lib/rabbitmq-client.jar** to **/opt/kairosdb/lib/rabbitmq-client.jar** (This is the official RabbitMQ Java client. Latest version is 3.5.5)

## 2. Configuration ##

Plugin has one configuration file **kairosdb-rabbitmq.properties**. There you can define different RabbitMQ parameters.

    kairosdb.plugin.rabbitmq.host = localhost
    kairosdb.plugin.rabbitmq.queue = kairosdb
    kairosdb.plugin.rabbitmq.rejected.exchange = kairosdb.rejected
    kairosdb.plugin.rabbitmq.rejected.queue = kairosdb.rejected
    kairosdb.plugin.rabbitmq.virtualhost = /
    kairosdb.plugin.rabbitmq.username = guest
    kairosdb.plugin.rabbitmq.password = guest
    kairosdb.plugin.rabbitmq.port = -1
    kairosdb.plugin.rabbitmq.connectionTimeout = 0
    kairosdb.plugin.rabbitmq.requestedChannelMax = 0
    kairosdb.plugin.rabbitmq.requestedFrameMax = 0
    kairosdb.plugin.rabbitmq.requestedHeartbeat = 0

## 3. Message formats ##

### JSON ###

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
    
CSV is built in format: metricname;tags1;values1;tags2;values2. Each section is divided by ;. tags and values are split by comma and represented as key:value.

## 4. Rejected messages ##

In case if the message cannot be consumed, then it's forwarded to rejected messages exchange. This exchange is configurable in .properties file.
