package main

import (
	"flag"
	"log"

	"github.com/streadway/amqp"
)

func main() {
	message := flag.String("m", "hello", "message to be send")
	flag.Parse()

	send(*message)
}

func send(msg string) {
	URI := "amqp://guest:guest@localhost:5672"
	queue := "task_queue"

	connection, channel, queueName := connect(URI, queue)
	defer func() {
		channel.Close()
		connection.Close()
	}()

	err := channel.Publish(
		"",
		queueName,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(msg),
		},
	)
	checkError(err, "Error on Publish a message:")

	log.Println("Published!")
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

func checkError(err error, msg string) {
	if err != nil {
		log.Fatalln(msg, err.Error())
		return
	}
}
