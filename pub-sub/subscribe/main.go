package main

import (
	"log"

	"github.com/streadway/amqp"
)

func main() {
	receive()
}

func receive() {
	URI := "amqp://guest:guest@localhost:5672"
	exchangeName := "logs"
	connection, channel, queue := connect(URI)
	defer func() {
		channel.Close()
		connection.Close()
	}()

	err := channel.ExchangeDeclare(
		exchangeName,
		"fanout",
		true,
		false,
		false,
		false,
		nil,
	)
	checkError(err, "Error on declare Exchange:")

	err = channel.QueueBind(
		queue.Name,
		"",
		exchangeName,
		false,
		nil,
	)
	checkError(err, "Error on bind queue:")

	msgs, err := channel.Consume(
		queue.Name,
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

func processMessage(msg amqp.Delivery) {
	log.Printf("%s\n", msg.Body)
}

func connect(URI string) (*amqp.Connection, *amqp.Channel, amqp.Queue) {
	connection, err := amqp.Dial(URI)
	checkError(err, "Error on connect:")
	// defer connection.Close()

	channel, err := connection.Channel()
	checkError(err, "Error on get channel:")
	// defer channel.Close()

	q, err := channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	checkError(err, "Error on declare queue:")

	return connection, channel, q
}

func checkError(err error, msg string) {
	if err != nil {
		log.Fatalln(msg, err.Error())
		return
	}
}
