package main

import (
	"bytes"
	"log"
	"time"

	"github.com/streadway/amqp"
)

func main() {
	receive()
}

func receive() {
	URI := "amqp://guest:guest@localhost:5672"
	queue := "task_queue"

	connection, channel, queueName := connect(URI, queue)
	defer func() {
		channel.Close()
		connection.Close()
	}()

	err := channel.Qos(
		1, // how many messages
		0, // how many bytes
		false,
	)
	checkError(err, "Error on setting QoS:")

	msgs, err := channel.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	checkError(err, "Error on Consume queue:")

	for msg := range msgs {
		go processMessage(msg)
	}
}

func connect(URI string, queue string) (*amqp.Connection, *amqp.Channel, string) {
	connection, err := amqp.Dial(URI)
	checkError(err, "Error on connect:")
	// defer connection.Close()

	channel, err := connection.Channel()
	checkError(err, "Error on get channel:")
	// defer channel.Close()

	q, err := channel.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	checkError(err, "Error on declare queue:")

	return connection, channel, q.Name
}

func processMessage(message amqp.Delivery) {
	log.Printf("received message: %s\n", message.Body)
	counter := bytes.Count(message.Body, []byte("."))
	time.Sleep(time.Duration(counter) * time.Second)
	log.Println("done")
	message.Ack(false)
}

func checkError(err error, msg string) {
	if err != nil {
		log.Fatalln(msg, err.Error())
		return
	}
}
