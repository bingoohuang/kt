# kt - a Kafka tool that likes JSON [![Build Status](https://travis-ci.org/bingoohuang/kt.svg?branch=master)](https://travis-ci.org/bingoohuang/kt)

## kt 使用简介

1. 通用设置 brokers 和 topic
    1. 通过环境变量 `export KT_BROKERS=192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092; export KT_TOPIC=elastic.backup`
    2. 通过命令参数 `kt tail -brokers=192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092 -topic elastic.backup`，不方便的是，导致命令过长，每次执行，都得带上这两个参数
2. 消费最新消息 `kt tail`
3. 生产消息
    1. 直接消息：`echo '你要发送的消息载荷' |  kt produce -literal`
    2. 指定 key 和 partition ，`echo '{"key": "id-23", "value": "消息载荷", "partition": 0}' | kt produce -topic greetings`
    3. 使用 JJ 命令生成随机消息：`JJ_N=3 jj -gu a=@姓名 b=@汉字 c=@性别 d=@地址 e=@手机 f=@身份证 g=@发证机关 h=@邮箱 i=@银行卡 j=@name k=@ksuid l=@objectId m='@random(男,女)' n='@random_int(20-60)' o='@random_time(yyyy-MM-dd)' p=@random_bool q='@regex([a-z]{5}@xyz[.]cn)' |  kt produce -literal`
    4. 从文件中读取,每一行作为一个消息： `cat p20w.txt | kt produce -literal -stats`
4. 生产消息性能压测
    1. 随机字符串写入压测 `kt perf`
    2. 使用 JSON 模板生成写入压测： `kt perf -msg-json-template '{"id":"@objectId","sex":"@random(male,female)"}'`
5. 其它，看帮助
    1. 子命令列表：`kt help`
    2. 子命令帮助：`kt tail -help`

## 示例日志

```sh
# kt tail -brokers=192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092 -topic elastic.backup
topic: elastic.backup offset: 42840172 partition: 1 key:  timestamp: 2022-07-06 09:16:29.011 valueSize: 100B msg: {"partition":1,"offset":42840172,"value":"AHn3XiZADEPb1UG36b3Eh3yEM84csGvMgJ77A8cJyRiue5FeQQwBH9PeZILJT2MIWZlgTUllCiYFT2Xdi1n4mJsbKtdz5hoqkenj","timestamp":"2022-07-06T09:16:29.011+08:00"}
topic: elastic.backup offset: 43249889 partition: 0 key:  timestamp: 2022-07-06 09:16:29.011 valueSize: 100B msg: {"partition":0,"offset":43249889,"value":"ufLYBbGHJ6okJoziJOcTtKwNQECXdAwczyoSGSYl3prCHpKQJdGlW6p3l3d7S6pYe9clGkt0zoJ2fBnYdNPhjPPgC7JBwA1rCt2V","timestamp":"2022-07-06T09:16:29.011+08:00"}
topic: elastic.backup offset: 42835575 partition: 2 key:  timestamp: 2022-07-06 09:16:29.011 valueSize: 100B msg: {"partition":2,"offset":42835575,"value":"oubuyjAFVdCoN0aB4lJHgYnagkOg3Ivf8zT0Ui5SEotX9SsAqv4VTbQtcSvC2AKIms50VioUa7DpJJBDQOIOjCHjjmcCB4SvOMBU","timestamp":"2022-07-06T09:16:29.011+08:00"}
^C
# export KT_BROKERS=192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092
# export KT_TOPIC=elastic.backup
# kt tail
topic: elastic.backup offset: 43249889 partition: 0 key:  timestamp: 2022-07-06 09:16:29.011 valueSize: 100B msg: {"partition":0,"offset":43249889,"value":"ufLYBbGHJ6okJoziJOcTtKwNQECXdAwczyoSGSYl3prCHpKQJdGlW6p3l3d7S6pYe9clGkt0zoJ2fBnYdNPhjPPgC7JBwA1rCt2V","timestamp":"2022-07-06T09:16:29.011+08:00"}
topic: elastic.backup offset: 42840172 partition: 1 key:  timestamp: 2022-07-06 09:16:29.011 valueSize: 100B msg: {"partition":1,"offset":42840172,"value":"AHn3XiZADEPb1UG36b3Eh3yEM84csGvMgJ77A8cJyRiue5FeQQwBH9PeZILJT2MIWZlgTUllCiYFT2Xdi1n4mJsbKtdz5hoqkenj","timestamp":"2022-07-06T09:16:29.011+08:00"}
topic: elastic.backup offset: 42835575 partition: 2 key:  timestamp: 2022-07-06 09:16:29.011 valueSize: 100B msg: {"partition":2,"offset":42835575,"value":"oubuyjAFVdCoN0aB4lJHgYnagkOg3Ivf8zT0Ui5SEotX9SsAqv4VTbQtcSvC2AKIms50VioUa7DpJJBDQOIOjCHjjmcCB4SvOMBU","timestamp":"2022-07-06T09:16:29.011+08:00"}
```

```sh
$ kt perf-produce
50000 records sent, 98584.6 records/sec (9.40 MiB/sec ingress, 4.93 MiB/sec egress), 209.7 ms avg latency, 161.2 ms stddev, 191.0 ms 50th, 369.5 ms 75th, 429.0 ms 95th, 429.0 ms 99th, 429.0 ms 99.9th, 0 total req. in flight

$ kt perf-produce -msg-json-template '{"id":"@objectId","sex":"@random(male,female)"}'
50000 records sent, 608952.2 records/sec (58.07 MiB/sec ingress, 5.23 MiB/sec egress), 164.1 ms avg latency, 170.8 ms stddev, 119.0 ms 50th, 405.8 ms 75th, 420.0 ms 95th, 420.0 ms 99th, 420.0 ms 99.9th, 0 total req. in flight
```

Some reasons why you might be interested:

* Consume messages on specific partitions between specific offsets.
* Display topic information (e.g., with partition offset and leader info).
* Modify consumer group offsets (e.g., resetting or manually setting offsets per topic and per partition).
* JSON output for easy consumption with tools like [kp](https://github.com/echojc/kp) or [jq](https://stedolan.github.io/jq/).
* JSON input to facilitate automation via tools like [jsonify](https://github.com/fgeller/jsonify).
* Configure brokers, topic and authentication via environment variables `KT_BROKERS`, `KT_TOPIC` and `KT_AUTH`.
* Fast start up time.
* No buffering of output.
* Binary keys and payloads can be passed and presented in base64 or hex encoding.
* Support for TLS authentication.
* Basic cluster admin functions: Create & delete topics.

I'm not using kt actively myself anymore, so if you think it's lacking some feature - please let me know by creating an issue!

## Examples

<details><summary>Read details about topics that match a regex</summary>

```sh
$ kt topic -filter news -partitions
{
  "name": "actor-news",
  "partitions": [
    {
      "id": 0,
      "oldest": 0,
      "newest": 0
    }
  ]
}
```
</details>

<details><summary>Produce messages</summary>

```sh
$ echo 'Alice wins Oscar' | kt produce -topic actor-news -literal
{
  "count": 1,
  "partition": 0,
  "startOffset": 0
}
$ echo 'Bob wins Oscar' | kt produce -topic actor-news -literal
{
  "count": 1,
  "partition": 0,
  "startOffset": 0
}
$ for i in {6..9} ; do echo Bourne sequel $i in production. | kt produce -topic actor-news -literal ;done
{
  "count": 1,
  "partition": 0,
  "startOffset": 1
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 2
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 3
}
{
  "count": 1,
  "partition": 0,
  "startOffset": 4
}
```
</details>

<details><summary>Or pass in JSON object to control key, value and partition</summary>

```sh
$ echo '{"value": "Terminator terminated", "key": "Arni", "partition": 0}' | kt produce -topic actor-news
{
  "count": 1,
  "partition": 0,
  "startOffset": 5
}
```
</details>

<details><summary>Read messages at specific offsets on specific partitions</summary>

```sh
$ kt consume -topic actor-news -offsets 0=1:2
{
  "partition": 0,
  "offset": 1,
  "key": "",
  "value": "Bourne sequel 6 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
{
  "partition": 0,
  "offset": 2,
  "key": "",
  "value": "Bourne sequel 7 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
```
</details>

<details><summary>Follow a topic, starting relative to newest offset</summary>

```sh
$ kt consume -topic actor-news -offsets all=newest-1:
{
  "partition": 0,
  "offset": 4,
  "key": "",
  "value": "Bourne sequel 9 in production.",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
{
  "partition": 0,
  "offset": 5,
  "key": "Arni",
  "value": "Terminator terminated",
  "timestamp": "1970-01-01T00:59:59.999+01:00"
}
^Creceived interrupt - shutting down
shutting down partition consumer for partition 0
```
</details>

<details><summary>View offsets for a given consumer group</summary>

```sh
$ kt group -group enews -topic actor-news -partitions 0
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 6,
      "lag": 0
    }
  ]
}
```
</details>

<details><summary>Change consumer group offset</summary>

```sh
$ kt group -group enews -topic actor-news -partitions 0 -reset 1
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 1,
      "lag": 5
    }
  ]
}
$ kt group -group enews -topic actor-news -partitions 0
found 1 groups
found 1 topics
{
  "name": "enews",
  "topic": "actor-news",
  "offsets": [
    {
      "partition": 0,
      "offset": 1,
      "lag": 5
    }
  ]
}
```
</details>

<details><summary>Create and delete a topic</summary>

```sh
$ kt admin -topic.create morenews -topic.config $(jsonify =NumPartitions 1 =ReplicationFactor 1)
$ kt topic -filter news
{
  "name": "morenews"
}
$ kt admin -topic.delete morenews
$ kt topic -filter news
```

</details>

<details><summary>Change broker address via environment variable</summary>

```sh
$ export KT_BROKERS=brokers.kafka:9092
$ kt <command> <option>
```

</details>

## Installation

You can download kt via the [Releases](https://github.com/fgeller/kt/releases) section.

Alternatively, the usual way via the go tool, for example:

    $ go get -u github.com/fgeller/kt

Or via Homebrew on OSX:

    $ brew tap fgeller/tap
    $ brew install kt

### Docker

[@Paxa](https://github.com/Paxa) maintains an image to run kt in a Docker environment - thanks!

For more information: [https://github.com/Paxa/kt](https://github.com/Paxa/kt)

## Usage:

    $ kt -help
    kt is a tool for Kafka.

    Usage:

            kt command [arguments]

    The commands are:

            consume        consume messages.
            produce        produce messages.
            topic          topic information.
            group          consumer group information and modification.
            admin          basic cluster administration.

    Use "kt [command] -help" for for information about the command.

    Authentication:

    Authentication with Kafka can be configured via a JSON file.
    You can set the file name via an "-auth" flag to each command or
    set it via the environment variable KT_AUTH.

## Authentication / Encryption

Authentication configuration is possibly via a JSON file. You indicate the mode
of authentication you need and provide additional information as required for
your mode. You pass the path to your configuration file via the `-auth` flag to
each command individually, or set it via the environment variable `KT_AUTH`.

### TLS

Required fields:

 - `mode`: This needs to be set to `TLS`
 - `client-cert`: Path to your certificate
 - `client-cert-key`: Path to your certificate key
 - `ca-cert`: Path to your CA certificate

Example for an authorization configuration that is used for the system tests:


    {
        "mode": "TLS",
        "client-cert": "testdata/test-secrets/kt-test.crt",
        "client-cert-key": "testdata/test-secrets/kt-test.key",
        "ca-cert": "testdata/test-secrets/snakeoil-ca-1.crt"
    }

### TLS one-way

Required fields:

 - `mode`: This needs to be set to `TLS-1way`

Example:


    {
        "mode": "TLS-1way",
    }

### Other modes

Please create an [issue](https://github.com/bingoohuang/kt/issues/new) with details for the mode that you need.

### handy scripts

1. `kt consume -brokers 192.168.18.14:9092 -topic metrics -version 0.10.0.0`
