## Kafka message size

For example, if your message size is 200MB (209715200 bytes)
And what you should to do, step by step, fork
from [ignatenko-denis/kafka-message-max-bytes](https://github.com/ignatenko-denis/kafka-message-max-bytes)

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

## kafka实战 - 处理大文件需要注意的配置参数

https://www.cnblogs.com/MyOnlyBook/p/10035670.html

### 概述

kafka配置参数有很多，可以做到高度自定义。但是很多用户拿到kafka的配置文件后，基本就是配置一些host，port，id之类的信息，
其他的配置项采用默认配置，就开始使用了。这些默认配置是经过kafka官方团队经过严谨宽泛的测试之后，求到的最优值。在单条信息很小，
大部分场景下都能得到优异的性能。但是如果想使用kafka存储一些比较大的，比如100M以上的数据，这些默认的配置参数就会出现各种各样的问题。

我们的业务是数据大小没有什么规律，小的只有几kb，大的可能有几百M。为了使得整体架构简洁统一，降低维护成本，这些大小各异的样本都需要流经kafka。
这就要求把kafka的一些默认配置自定义，才能正确运行。这些配置可以分为3大块，producer端的， broker端的，consumer端的。
使用的kafka为0.10.2.0，以下的讨论也只在这个版本做过测试。producer和consumer均使用php client rdkafka。

### broker端的配置

`message.max.bytes`，默认是1M。[如果启用了压缩，则是压缩后的消息大小](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/common/config/TopicConfig.java#L94)。
决定了broker可以接受多大的数据。如果采用默认配置，producer生产1M以上的数据都会被broker丢掉。
所以这个参数需要设置为单条消息的最大大小。和这个参数相关的还有一个topic级别的 `max.message.bytes`，其实它和 `message.max.bytes` 是一个功能，
只是针对topic的设置，只对单个topic有作用，不会影响到其他topic（其他topic仍然使用message.max.bytes）。

`replica.fetch.max.bytes`， 默认也是1M。这个参数的描述是replica的FETCH请求从每个partition获取数据的最大大小。如果把 `message.max.bytes` 设置为100M，
那topic中就会有100M大小的数据。但是replica的FETCH请求最大大小却是默认的1M。这样造成的后果就是producer虽然成功了，但是数据没法复制出去，kafka的备份功能就形同虚设了。
但是刚才说的问题只有在0.8.2.x及以前的版本才会出现。在0.8.2.x之后的版本，即便 `replica.fetch.max.bytes`采用默认值，也可以进行复制。FETCH请求是批量进行的，
replica会发过来类似的请求 `topic_name : [ partition1, partition2, partition3 ]` 来进行批量复制。

在0.8.2.x及以前版本中，如果 `replica.fetch.max.bytes` 小于碰到的第一条数据，那leader_broker会返回错误，而replica会不断重试，但是永远也成功不了，
造成的后果就是broker之前的流量暴增，影响到真正有用的逻辑，但是实际上传输的都是重试信息。 在0.8.2.x之后，这个bug被修复，如果 `replica.fetch.max.bytes`
小于碰到的第一条数据，会停止去其他的partition继续获取数据，直接把这条数据返回。 可以明显地看到，功能虽然保住了，但是可能会造成如下2个问题：

1. 批量复制退化成单条复制。假设有broker1和broker2，broker2复制broker1。如果broker1上面有很多partition，那复制的过程就是一个一个partition地复制，效率可想而知。
2. 假设partition1增长地很快，而且单条消息都超过了
   replica.fetch.max.bytes。但是partition2和partition3增长地没有partition1快。那么每次都只会直接返回partition1的第一条需要复制的数据，partition2和partition3的永远都没有机会复制。

不过第二个问题官网上说已经被解决了，会把请求复制中partition的顺序随机打乱，让每个partition都有机会成为第一个被复制的partition。但是笔者没有做过测试，是否真地解决了还不是很清楚。

所以综合第一个和第二个问题，这个参数还是手动设置一下比较好，设置为比message.max.bytes稍大一些。这样批量复制退化为单条复制这种问题会在很大程度上缓解，而且第二个问题也不会再出现。

相关的讨论可以在这里找到：

- https://blog.csdn.net/guoyuqi0554/article/details/48630907
- https://issues.apache.org/jira/browse/KAFKA-1756
- https://cwiki.apache.org/confluence/display/KAFKA/KIP-74%3A+Add+Fetch+Response+Size+Limit+in+Bytes

`log.segment.bytes`，默认是1G。确保这个值大于单条数据的最大大小即可。

bin目录下的kafka-run-class.sh中需要配置的参数，Brokers allocate a buffer the size of replica.fetch.max.bytes for each partition they
replicate. If replica.fetch.max.bytes is set to 1 MiB, and you have 1000 partitions, about 1 GiB of RAM is required.
Ensure that the number of partitions multiplied by the size of the largest message does not exceed available memory. The
same consideration applies for the consumer fetch.message.max.bytes setting. Ensure that you have enough memory for the
largest message for each partition the consumer replicates. With larger messages, you might need to use fewer partitions
or provide more RAM。

kafka本身运行在JVM上，如果设置的replica.fetch.max.bytes很大，或者partition很多，则需要调整 `-Xms2048m, -Xmx2048m和--XX:MaxDirectMemorySize=8192m`，
前2者调的过小，kafka会出现java.lang.OutOfMemoryError: Java heap space的错误。第3个配置的太小，kafka会出现java.lang.OutOfMemoryError: Direct
buffer memory的错误。

### producer端的配置

`message.max.byte`， 最大可发送长度。如果这个配置小于当前要发送的单个数据的大小，代码会直接抛异常Uncaught exception 'RdKafka\Exception' with message 'Broker:
Message size too large'，请求也不会发送到broker那里。

`socket.timeout.ms`， 默认为60000ms，网络请求的超时时间。

`message.timeout.ms`，默认为300000ms，消息发送timeout。client的send调用返回并不一定是已经把消息发送出去了。client这一端其实会攒buffer，然后批量发。一个消息如果在特定时间（min(
socket.timeout.ms, message.timeout.ms )）内没有被发出去，那么当回调被调用时，会得到“time
out”的错误。这个参数和上面的socket.timeout.ms在网络情况不好，或者发送数据非常大的时候需要设置一下。不过一般的工作环境是在内网，使用默认配置一般不会出现什么问题。

### consumer端的配置

`fetch.max.bytes`，
这个参数决定了可以成功消费到的最大数据。比如这个参数设置的是10M，那么consumer能成功消费10M以下的数据，但是最终会卡在消费大于10M的数据上无限重试。fetch.max.bytes一定要设置到大于等于最大单条数据的大小才行。

`receive.message.max.bytes`
，一般在C/S架构下，C和S都是通过一种特殊的协议进行通信的，kafka也不例外。fetch.max.bytes决定的只是response中纯数据的大小，而kafka的FETCH协议最大会有512字节的协议头，所以这个参数一般被设置为fetch.max.bytes+512。

`session.timeout.ms`，默认是10000ms，会话超时时间。当我们使用consumer_group的模式进行消费时，kafka如果检测到某个consumer挂掉，就会记性rebalance。consumer每隔一段时间(
heartbeat.interval.ms)
给broker发送心跳消息，如果超过这个时间没有发送，broker就会认为这个consumer挂了。这个参数的有效取值范围是broker端的设置group.min.session.timeout.ms(6000)
和group.max.session.timeout.ms(300000)之间。

`max.poll.interval.ms`,
默认是300000ms，也是检测consumer失效的timeout，这个timeout针对地是consumer连续2次从broker消费消息的时间间隔。为什么有了session.timeout.ms又要引入max.poll.interval.ms？
在kafka 0.10.0
之前，consumer消费消息和发送心跳信息这两个功能是在一个线程中进行的。这样就会引发一个问题，如果某条数据process的时间较长，那么consumer就无法给broker发送心跳信息，broker就会认为consumer死了。所以不得不提升session.timeout.ms来解决这个问题。但是这又引入了另外一个问题，如果session.timeout.ms设置得很大，那么检测一个consumer挂掉的时间就会很长，如果业务是实时的，那这就是不能忍受的。所以在
0.10.0
之后，发送心跳信息这个功能被拎出来在单独的线程中做，session.timeout.ms就是针对这个线程到底能不能按时发送心跳的。但是如果这个线程运行正常，但是消费线程挂了呢？这就无法检测了啊。所以就引进了max.poll.interval.ms，用来解决这个问题。所以如果使用比较新的producer库，恰好有些数据处理时间比较长，就可以适当增加这个参数的值。但是这个配置在php的client没有找到，应该是不支持。具体怎么实现这个参数的功能，还有待学习更新。但是java
client可以配置这个参数。

关于 `max.poll.interval.ms`
的讨论：https://stackoverflow.com/questions/39730126/difference-between-session-timeout-ms-and-max-poll-interval-ms-for-kafka-0-10-0

### 确保数据安全性的配置

producer端可以配置一个叫 `acks` 的参数。代表的是broker再向producer返回写入成功的response时，需要确保写入ISR broker的个数。
0表示broker不用返回response，1表示broker写入leader后即返回，-1表示broker写入所有ISR后返回。

如果想让数据丢失的可能性降到最小，就设置acks=-1。让broker把数据写入所有的副本之后返回。

但是有一个问题，如果ISR中，除了leader，剩下的副本全挂了怎么办？这样即便我们设置acks=-1， 也只是写入leader就返回，我们什么都不知道，
还以为是写入了所有的副本才返回写入成功的。为了解决这个问题，kafka在broker端引入了一个配置，min.insync.replicas。
如果acks设置为-1，但是写入ISR的个数小于min.insync.replicas配置的个数，则producer代码会抛出NotEnoughReplicas让开发人员指导出现了问题。
