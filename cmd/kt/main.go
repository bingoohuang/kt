package main

import (
	"fmt"
	"os"

	"github.com/bingoohuang/gg/pkg/v"
	_ "github.com/bingoohuang/godaemon/autoload"
	. "github.com/bingoohuang/kt/pkg/kt"
	_ "github.com/joho/godotenv/autoload"
)

var usageMessage = fmt.Sprintf(`kt is a tool for Kafka.
Usage:
	kt command [arguments]

The commands are:
	consume/tail consume messages.
	produce      produce messages.
	perf-produce produce messages performance testing.
	kiss-consume KISS consume messages.
	kiss-produce KISS produce messages.
	topic        topic information.
	group        consumer group information and modification.
	admin        basic cluster administration.
	version      print program version and exit.

Use "kt [command] -help" for more information about the command.
Use "kt -version" for details on what version you are running.

Authentication:
Authentication with Kafka can be configured via a JSON file.
You can set the file name via an "-auth" flag to each command or
set it via the environment variable %s.

You can find more details at https://github.com/bingoohuang/kt

Ussage demo:
1. 通用设置 brokers 和 topic
    1. 通过环境变量: export KT_BROKERS=192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092 KT_TOPIC=topic.test KT_VERSION=0.10.0.0 KT_AUTH=usr:user,pwd:123123
    2. 通过命令参数: kt tail -brokers=192.168.1.1:9092,192.168.1.2:9092,192.168.1.3:9092 -topic elastic.backup -version 0.10.0.0，不方便的是，导致命令过长，每次执行，都得带上这两个参数
2. 消费最新消息: kt tail (环境变量 CLEAN_MSG=1 清除转移符号）
3. 生产消息
    1. 直接消息：echo '你要发送的消息载荷' |  kt produce -literal -topic greetings
    2. 指定 key 和 partition : echo '{"key": "id-23", "value": "消息载荷", "partition": 0}' | kt produce -topic greetings
    3. 使用 JJ 命令生成随机消息：N=3 jj -gu a=@姓名 b=@汉字 c=@性别 d=@地址 e=@手机 f=@身份证 g=@发证机关 h=@邮箱 i=@银行卡 j=@name k=@ksuid l=@objectId m='@random(男,女)' n='@random_int(20-60)' o='@random_time(yyyy-MM-dd)' p=@random_bool q='@regex([a-z]{5}@xyz[.]cn)' |  kt produce -literal -topic greetings
    4. 从文件中读取,每一行作为一个消息： cat p20w.txt | kt produce -literal -stats
4. 生产消息性能压测
    1. 随机字符串写入压测: kt perf
    2. 使用 JSON 模板生成写入压测： kt perf -json-template '{"id":"@objectId","sex":"@random(male,female)"}'
5. 其它，看帮助
    1. 子命令列表：kt help
    2. 子命令帮助，例如：kt tail -help
`, EnvAuth)

func parseArgs() command {
	if len(os.Args) < 2 {
		failf(usageMessage)
	}

	switch os.Args[1] {
	case "consume", "consumer", "tail":
		return &consumeCmd{}
	case "console-consume", "console-consumer", "console-tail":
		return &consoleConsumerCmd{}
	case "console-produce", "console-producer":
		return &consoleProducerCmd{}
	case "produce", "producer":
		return &produceCmd{}
	case "perf", "perf-produce", "perf-producer":
		return &perfProduceCmd{}
	case "topic":
		return &topicCmd{}
	case "group":
		return &groupCmd{}
	case "kiss-consume", "kiss-consumer":
		return &kissConsumer{}
	case "kiss-produce", "kiss-producer":
		return &kissProducer{}
	case "admin":
		return &adminCmd{}
	case "-h", "-help", "--help":
		quitf(usageMessage)
	case "-version", "--version", "version", "-v", "--v", "v":
		quitf(v.Version())
	default:
		failf(usageMessage)
	}
	return nil
}

func main() {
	cmd := parseArgs()
	cmd.run(os.Args[2:])
}
