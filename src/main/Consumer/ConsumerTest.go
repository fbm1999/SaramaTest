package Consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"time"
)

// standalone consumer 独立消费者

func MyConsume(brokers []string){

	//新建consumer
	consumer, err := sarama.NewConsumer(brokers,nil)
	if err != nil{
		panic(err)
	}

	// 关闭conumser
	defer func() {
		if err := consumer.Close();err !=nil{
			log.Println(err)
		}
	}()

	// 注册信号来接受中断信号
	signals := make(chan os.Signal,1)
	signal.Notify(signals,os.Interrupt)


	// 指定要消费的主题、分区、起始offset
	// OffsetOldest:一个分区上面可用的最旧偏移量
	// OffsetNewest:一个分区上面最新的偏移量（LEO）
	partitionConsumer, err := consumer.ConsumePartition("test",0,sarama.OffsetOldest) // ConsumePartition方法返回一个PartitionConsumer，用于消费指定主题的指定分区，并指定消费开始的offset

	// 循环消费
	var consumed int // 记录消费的数目
	loop:
		for{
			select {
			case msg := <- partitionConsumer.Messages():
				time.Sleep(time.Second*2)
				fmt.Printf("key=%s value=%s offset=%d partition=%d \n", msg.Key,msg.Value,msg.Offset,msg.Partition)
				consumed++
			case <- signals:
				break loop
			}
		}
	log.Printf("the consumer has consumed:%d",consumed)
}

