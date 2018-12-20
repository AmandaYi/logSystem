package main

import (
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
)

// 测试kafka
func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	client, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Println("producer close, err:", err)
		return
	}
	defer client.Close()
	count := 0
	for {
		count++
		msg := &sarama.ProducerMessage{}
		msg.Topic = "test"
		msg.Value = sarama.StringEncoder("这是第" + strconv.Itoa(count) + "条数据, 用来测试,请注意查收!,topic的名字是test")
		pid, offset, err := client.SendMessage(msg)
		if err != nil {
			continue
		}
		fmt.Printf("pid:%v offset:%v\n", pid, offset)

		// time.Sleep(10 * time.Millisecond / 100000000000000)
	}

}
