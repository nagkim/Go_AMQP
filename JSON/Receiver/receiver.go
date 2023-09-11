package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/interconnectedcloud/go-amqp"
)

type Foo struct {
	StringArray []string  `json:"stringArray"`
	IntArray    []int     `json:"intArray"`
	FloatArray  []float32 `json:"floatArray"`
}

func connectToBroker(address string, username string, password string) (*amqp.Client, error) {
	client, err := amqp.Dial(address,
		amqp.ConnSASLPlain(username, password),
	)
	if err != nil {
		log.Printf("Failed to connect to %s: %s\n", address, err)
		return nil, err
	}
	return client, nil
}

func main() {
	masterBroker := "amqp://10.37.129.2:61616"

	username := "admin"
	password := "admin"
	queueName := "test/java"
	var client *amqp.Client
	var err error

	for {
		client, err = connectToBroker(masterBroker, username, password)
		if err != nil {
			// Retry
			time.Sleep(time.Second)
			continue
		}

		defer client.Close()

		adminSession, err := client.NewSession()
		if err != nil {
			log.Fatal("Creating session:", err)
		}
		defer adminSession.Close(nil)

		receiver, err := adminSession.NewReceiver(
			amqp.LinkSourceAddress(queueName),
		)
		if err != nil {
			log.Fatal("Creating receiver:", err)
		}
		defer receiver.Close(nil)

		ctx := context.Background()

		for {
			message, err := receiver.Receive(ctx)
			if err != nil {
				log.Println("Receiving message:", err)
				continue
			}
			receivedJSON := string(message.GetData())
			log.Printf("Received JSON size: %d bytes\n", len(receivedJSON))

			//startTime := time.Now() // Start the timer
			//log.Println("Received JSON:", receivedJSON)

			var obj Foo
			if err := json.Unmarshal([]byte(receivedJSON), &obj); err != nil {
				log.Println("JSON Unmarshaling error:", err)
			} else {
				//elapsedTime := time.Since(startTime)
				//log.Printf("Unmarshaling took %.3f ms\n", float64(elapsedTime.Milliseconds()))
				log.Printf("Received JSON: %+v\n", obj)

			}

			message.Accept()
		}
	}
}
