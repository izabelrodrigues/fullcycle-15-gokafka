package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)


func main(){
	
	deliveryChannel := make(chan kafka.Event)
	producer := NewKafkaProducer()
	Publish("transferiu", "teste", producer, []byte("transferencia"), deliveryChannel)

	// e := <-deliveryChannel

	// msg := e.(*kafka.Message)

	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println("Erro ao enviar mensagem")
	// } else {
	// 	fmt.Println("Mensagem enviada com sucesso: ", msg.TopicPartition)
	// }

	go DeliveryReport(deliveryChannel)
	producer.Flush(5000)
}

func NewKafkaProducer() *kafka.Producer{

	configMap := &kafka.ConfigMap{
		"bootstrap.servers":"kafka:9092",
		"delivery.timeout.ms": "0", // aguarda o retorno infinitamente
		"acks":"all",
		"enable.idempotence": "true",
	}

	p, err := kafka.NewProducer(configMap)

	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChannel chan kafka.Event) error {

	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: key,
	}

	err := producer.Produce(message, deliveryChannel)

	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChannel chan kafka.Event) {

	for e := range deliveryChannel {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar mensagem")
			} else {
				fmt.Println("Mensagem enviada com sucesso: ", ev.TopicPartition)
			}
		}
	}

}
