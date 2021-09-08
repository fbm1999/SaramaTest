package main

func main(){
	brokers := []string{"127.0.0.1:9092","127.0.0.1:9093","127.0.0.1:9094"}

	// 异步发送消息（记得使用控制台中断信号如ctrl+c或者kill -9来中断程序）
	MyAsyncProduce(brokers)

	// 同步发送消息
	MySyncProduce(brokers)
}
