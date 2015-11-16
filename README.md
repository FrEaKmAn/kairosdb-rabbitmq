[![Build Status](https://travis-ci.org/FrEaKmAn/kairosdb-rabbitmq.svg)](https://travis-ci.org/FrEaKmAn/kairosdb-rabbitmq)

# kairosdb-rabbitmq
KairosDB plugin for RabbitMQ. Greatly inspired by https://github.com/hugocore/kairosdb-rabbitmq. Compared to previous plugin, this plugin uses one queue to handle incoming messages. Each message has a metric attribute identifying where to save datapoints.  

## 1. Install ##

Copy the files from dist/ to the KairosDB installation folder (if installed by dpkg it's /opt/kairosdb/):

1. **/dist/conf/kairosdb-rabbitmq.properties** to **/opt/kairosdb/conf/kairosdb-rabbitmq.properties**
2. **/dist/lib/kairosdbdb-rabbitmq.jar** to **/opt/kairosdb/lib/kairosdbdb-rabbitmq.jar**
3. **/dist/lib/rabbitmq-client.jar** to **/opt/kairosdb/lib/rabbitmq-client.jar** (This is the official RabbitMQ Java client. Latest version is 3.5.6)

## 2. RabbitMQ configuration

Plugin will by default declare a queue, rejected exchange and rejected queue. In case if you have this already configured, you can skip creation in the properties. 

You will have to create an exchange (to forward messages to main queue) manually and manually configure bindings. 

## 3. Plugin Configuration ##

Plugin has one configuration file **kairosdb-rabbitmq.properties**. There you can define different RabbitMQ parameters.

    kairosdb.plugin.rabbitmq.host = rabbitmq
    kairosdb.plugin.rabbitmq.queue = kairosdb
    kairosdb.plugin.rabbitmq.queue.declare = true
    kairosdb.plugin.rabbitmq.rejected.exchange = kairosdb.rejected
    kairosdb.plugin.rabbitmq.rejected.exchange.declare = true
    kairosdb.plugin.rabbitmq.rejected.queue = kairosdb.rejected
    kairosdb.plugin.rabbitmq.rejected.queue.declare = true
    kairosdb.plugin.rabbitmq.virtual.host = /
    kairosdb.plugin.rabbitmq.username = guest
    kairosdb.plugin.rabbitmq.password = guest
    kairosdb.plugin.rabbitmq.port = -1
    kairosdb.plugin.rabbitmq.connection.timeout = 0
    kairosdb.plugin.rabbitmq.requested.channel.max = 0
    kairosdb.plugin.rabbitmq.requested.frame.max = 0
    kairosdb.plugin.rabbitmq.requested.heartbeat = 0
    kairosdb.plugin.rabbitmq.default.content.type = application/json
    
Plugin will by default declare all the required exchanges and queues. To disable this, you can use `*.declare` setting for each queue and exchange and change it to false.

You can also define default content type (`application/json` or `text/csv`) for messages. If message doesn't contain property [content type](http://www.rabbitmq.com/releases/rabbitmq-java-client/current-javadoc/), it will automatically use default content type and default message parser.

## 4. Message formats ##

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
    
Make sure you define message content type as `application/json`.
    
### CSV ###

Not a true CSV format, but combination of it.

    sensor.1;name:flow,unit:m3/min;1388530800000:1.16,1388530805000:2.98;name:pressure,unit:barg;1388530800000:7.10,1388530805000:7.05
    
CSV is built in format: metricname;tags1;values1;tags2;values2. Each section is divided by ;. tags and values are split by comma and represented as key:value. You must define message content type as `text/csv`.

## 5. Rejected messages ##

In case if the message cannot be consumed, then it's forwarded to rejected messages exchange and rejected queue. Plugin by default creates both. In case if you want to ignore rejected messages, then set `kairosdb.plugin.rabbitmq.rejected.exchange` to blank.

    kairosdb.plugin.rabbitmq.host = rabbitmq
    kairosdb.plugin.rabbitmq.queue = kairosdb
    kairosdb.plugin.rabbitmq.queue.declare = true
    kairosdb.plugin.rabbitmq.rejected.exchange = 
    kairosdb.plugin.rabbitmq.rejected.exchange.declare = false
    kairosdb.plugin.rabbitmq.rejected.queue = 
    kairosdb.plugin.rabbitmq.rejected.queue.declare = false
    ...