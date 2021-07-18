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

func send(message string) {
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
	checkError(err, "Error on bind a queue to an exchange:")

	err = channel.Publish(
		exchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "plain/text",
			Body:        []byte(message),
		},
	)
	checkError(err, "Error on Publish a message:")

	log.Println("Published!")
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
