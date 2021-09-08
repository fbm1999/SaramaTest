package main

import (
	main2 "SaramaTest/src/main/Consumer"
)
func main(){
	brokers := []string{"127.0.0.1:9092","127.0.0.1:9093","127.0.0.1:9094"}
	go main2.MyConsumerGroup(brokers,"group-1")
	go main2.MyConsumerGroup(brokers,"group-1")


	for{}
}

