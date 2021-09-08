package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"sync"
)

// 异步消息生产者:使用非阻塞API发送消息
// "You must read from the Errors（）channel or the producer will deadlock"  !一定要监听Errors管道，否则会死锁
// 这里的写法是使用WaitGroup来同步两个异步协程分别监听Successes和Errors通道，然后自身发送消息
// 也可以在同一个协程里面监听Errors通道并做错误处理

func MyAsyncProduce(Address []string) {
	config := sarama.NewConfig()
	config. Version = sarama.V2_1_0_0
	// 等待服务器所有副本都保存成功后响应,即acks=all(-1)
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 随机向partition发送消息
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	// 是否等待成功和失败后的响应，只有上面的RequireAcks设置不是NoReponse这里才有用.
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	// 设置使用的kafka版本，如果低于v0_10_0_0版本，消息中的timestrap没有作用，需要消息和生产同时配置
	config.Version = sarama.V2_8_0_0 // 一定要正确配置kafka版本
	//新建一个异步生产者
	asyncproducer, err := sarama.NewAsyncProducer(Address, config)
	if err != nil {
		fmt.Println(err)
		return
	}

	//	使用信号来进行优雅的关闭
	signals := make(chan os.Signal, 1)   // 创建一个传递os.Signal类型的管道
	signal.Notify(signals, os.Interrupt) // 注册信号

	var wg sync.WaitGroup                       // 同步工具，一个 WaitGroup 对象可以等待一组协程结束
	var enqueued, successes, producerErrors int // 统计指标，enqueued是入队次数、successes是成功数目，producerErrors是错误次数

	wg.Add(1) // worker协程+1，用来统计发送成功的数目
	go func() {
		// 从Successes管道中读取
		for suc := range asyncproducer.Successes() {
			fmt.Printf("发送成功 partition=%d offset=%d \n", suc.Partition, suc.Offset)
			successes++ //成功发送
		}
		defer wg.Done()
	}()

	wg.Add(1)
	go func() {
		for err := range asyncproducer.Errors() {
			fmt.Println(err)
			producerErrors++
		}
		defer wg.Done()
	}()

	// 弄了个label
Loop:
	//发送消息
	for i:=0;i<10000;i++{
		message := &sarama.ProducerMessage{
			Topic: "test",
			Value: sarama.StringEncoder("this is a message"),
			Key: sarama.StringEncoder(fmt.Sprintf("key%d",i)),
		}
		select {
		case asyncproducer.Input() <- message:
			// 睡2s发一个，不然太快了=-=
			//time.Sleep(time.Second*2)
			fmt.Println("消息进入input管道中")
			enqueued++
		case <-signals: // 接受到注册的信号后关闭生产者  中断信号的发送可以是CTRL+C或者kill或者kill -9命令
			asyncproducer.AsyncClose()
			break Loop // 在多重循环中使用label来标出想要break的循环
		}
	}

	wg.Wait() // 阻塞等待waitgroup中协程结束
	// 打印发送结果
	log.Printf("Successfully produced: %d; errors: %d \n", successes, producerErrors)
}
