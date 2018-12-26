package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/astaxie/beego/logs"
)

var (
	client sarama.SyncProducer
)

func InitKafka(addr string) (err error) {
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	// config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	client, err = sarama.NewSyncProducer([]string{addr}, config)
	if err != nil {
		logs.Error("kafka创建生产者失败,错误信息:", err)
		return
	}
	logs.Debug("kafka初始化成功!")
	return
}

func SendToKafka(data, topic string) (err error) {

	msg := &sarama.ProducerMessage{}
	msg.Topic = topic
	msg.Value = sarama.StringEncoder(data)
	partitioner, offset, err1 := client.SendMessage(msg)
	if err != nil {
		logs.Error("发送到kafka信息出现了错误,错误信息 => partitioner:%v offset:%v err:%v data:%v topic:%v", partitioner, offset, err, data, topic)
		err = err1
		return
	}
	return
}
