package Consumer

import (
	"context"
	"fmt"
	"github.com/Shopify/sarama"
	"log"
)

// 消费者组

func MyConsumerGroup(brokers []string,groupID string){
	// 配置
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0 // 指定kafka版本，默认设置为最近的稳定版本，若指定的kafka版本相对实际的旧一些，则可以向上兼容，但若是比实际的新，则可能会出现异常
	config.Consumer.Return.Errors = true

	// 新建consumerGroup
	consumerGroup, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil{
		panic(err)
	}
	// defer 关闭
	defer func() {
		consumerGroup.Close()
	}()

	// 一个协程监听Errors
	go func() {
		for err := range consumerGroup.Errors(){
			log.Println(err)
		}
	}()

	// 遍历consumer session
	ctx := context.Background()   //上下文context用于同步
	for{
		topics := []string{"test"}  // 订阅的主题列表
		handler := myConsumerGroupHandler{}

		// Consume方法为订阅的一组主题加入一组消费者，并且通过ConsumerGroupHandler启动一个阻塞的ConsumerGroupSession
		// 调用Consume方法时，每当遇到rebalance时，方法就会退出，因此调用方需要在一个无限循环中不停地调用Consume()
		// 真正的消费是在ConsumeClaim中执行的，因此需要实现ConsumerGroupHandler
		err := consumerGroup.Consume(ctx,topics,handler)
		if err != nil{
			panic(err)
		}
	}
}

// myConsumerGroupHandler 实现ConsumerGroupHandler接口
type myConsumerGroupHandler struct {}

func (mycgh myConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error{
	return nil
}

func (mycgh myConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession)error{
	return nil
}

// 每个partition与consumer的分配关系称为一个"claim" 一个partition只能被一个consumer消费

func (mycgh myConsumerGroupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession,claim sarama.ConsumerGroupClaim)error{
	// 消费消息
	for msg := range claim.Messages(){
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		sess.MarkMessage(msg,"")
	}
	return nil
}
