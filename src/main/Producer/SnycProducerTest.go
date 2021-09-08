package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"time"
)
// 同步模式：producer发送消息后会等待结果返回

func MySyncProduce(Address []string){
	// 配置
	config := sarama.NewConfig()

	// 这两个参数一定要设置为true
	config.Producer.Return.Successes = true  // 尤其是这个，从源码注释可以看到，该属性默认是disabled的。设置为true后，消息发送成功的通知会放在Success管道中
	config.Producer.Return.Errors = true
	config.Producer.Timeout = 5*time.Second // 等待消息返回的最大时间
	config.Version = sarama.V2_1_0_0
	// 新建SyncProducer  从源码上看，SyncProducer本质还是一个ASyncProducer
	syncProducer, err := sarama.NewSyncProducer(Address,config)  // config可以为nil，NewSyncProducer中会新建一个默认的config
	if err != nil{
		log.Printf("sarama.NewSynvProducer err,message=%s \n",err)
		return
	}
	// 使用defer语句，return前关闭连接
	defer syncProducer.Close()

	topic := "test"	// topic名称
	srcValue := "sync模式：this is a messgae. index=%d" // 消息值value的源格式

	// 发送消息
	for i:=0;i<10;i++{
		value := fmt.Sprintf(srcValue,i) // 要发送的消息
		// 封装进sarama提供的ProducerMessage中
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(value),   // 将value强制转换成ByteEncoder类型，ByteEncoder是定义的byte数组类型
		}
		// SendMessage方法可以发送消息/消息集合
		// 同步模式的关键就是SendMessage方法，SendMessage会阻塞直到Successes/Errors通道中有值为止，即收到发送成功/失败的值后才会返回，否则阻塞
		// 而异步模式是将消息发送到Input管道中，然后返回，不会阻塞。同时启动两个协程异步地分别监听Successer和Errors管道

		partition, offset, err := syncProducer.SendMessage(msg)
		if err!=nil{
			log.Printf("sendMessage:%s err=%s \n",value,err)
		}else{
			fmt.Printf( "%s 发送成功,partition=%d,offset=%d\n",value,partition,offset)
		}
		time.Sleep(2 * time.Second)
	}
}