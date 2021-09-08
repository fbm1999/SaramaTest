package main

import "SaramaTest/src/main/Consumer"

func main(){
	brokers := []string{"127.0.0.1:9092","127.0.0.1:9093","127.0.0.1:9094"}
	go Consumer.MyConsumerGroup(brokers,"group-1")
	go Consumer.MyConsumerGroup(brokers,"group-1")
	for{}
}
