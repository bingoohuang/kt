## Kafka message size

For example, if your message size is 200MB (209715200 bytes)
And what you should to do, step by step, fork from [ignatenko-denis/kafka-message-max-bytes](https://github.com/ignatenko-denis/kafka-message-max-bytes)

Used kafka_2.13-2.6.0

[Kafka Configuration](https://kafka.apache.org/documentation/#configuration)

# Kafka (broker)

in `server.properties`

```
message.max.bytes=209715200
replica.fetch.max.bytes=209715200
```

# Kafka topic

[Topic-Level Configs](https://kafka.apache.org/documentation/#topicconfigs)

topic-name - name of topic N - amount of partitions

This example creates a topic named my-topic with a custom max message size:

```sh
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic topic-name --partitions N --replication-factor N --config max.message.bytes=209715200
```

Overrides can also be changed or set later using the alter configs command. This example updates the max message size
for topic-name:

```sh
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name topic-name --alter --add-config max.message.bytes=209715200
```

Out message should be «Completed updating config for topic topic-name.

To check overrides set on the topic you can do:

```sh
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name topic-name --describe
```

To remove an override you can do:

```sh
bin/kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name topic-name --alter --delete-config max.message.bytes
```

# Kafka (producer)

in your application
configuration [application.yml](https://github.com/ignatenko-denis/kafka-template/blob/master/src/main/resources/config/application.yml#L19)

```yml
spring:
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: app-name
      auto-offset-reset: earliest
      properties:
        isolation:
          level: read_committed
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.ByteArrayDeserializer

    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.ByteArraySerializer
      properties:
        max.request.size: 209715200
        buffer.memory: 209715200
```

# Kafka Connect

See extended example about usage Kafka Connect in
repository [kafka-connect-jdbc](https://github.com/ignatenko-denis/kafka-connect-jdbc)

To avoid `java.lang.OutOfMemoryError: Java heap space`
in [connect-distributed.sh](https://github.com/ignatenko-denis/kafka-connect-jdbc/blob/main/kafka_2.13-2.6.0/bin/connect-distributed.sh#L30)
update memory configuration `export KAFKA_HEAP_OPTS="-Xms512M -Xmx4G"`

in [connect-distributed-jdbc.properties](https://github.com/ignatenko-denis/kafka-connect-jdbc/blob/main/kafka_2.13-2.6.0/config/connect-distributed-jdbc.properties)

```
bootstrap.servers=YOUR_KAFKA_HOST1:9092,YOUR_KAFKA_HOST2:9092
max.request.size=209715200
buffer.memory=209715200
producer.max.request.size=209715200
producer.buffer.memory=209715200
```

in [connect-mssql-source.json](https://github.com/ignatenko-denis/kafka-connect-jdbc/blob/main/kafka_2.13-2.6.0/config/connect-mssql-source.json)

```json
{
  "name": "mssql-connector",
  "config": {
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:sqlserver://127.0.0.1;databaseName=test_db;selectMethod=cursor;responseBuffering=adaptive",
    "connection.user": "test_user",
    "connection.password": "test_pwd",
    "dialect.name": "SqlServerDatabaseDialect",
    "topic.prefix": "mssql_",
    "table.whitelist": "test_table",
    "table.poll.interval.ms": 60000,
    "poll.interval.ms": 5000,
    "mode": "timestamp",
    "timestamp.column.name": "date",
    "tasks.max": 1,
    "batch.max.rows": 100,
    "errors.log.enable": true,
    "errors.log.include.messages": true,
    "errors.deadletterqueue.topic.name": "test_table-errors",
    "errors.tolerance": "all",
    "validate.non.null": false,
    "max.request.size": 209715200,
    "buffer.memory": 209715200
  }
}
```
