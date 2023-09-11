package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/interconnectedcloud/go-amqp"
)

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
			msg, err := receiver.Receive(ctx)
			if err != nil {
				log.Fatal(err)
			}

			startTime := time.Now() // Start the timer
			//log.Println("Received JSON:", receivedJSON)

			// Concatenate the message chunks to create a single byte slice
			var data []byte
			for _, chunk := range msg.Data {
				data = append(data, chunk...)
			}

			compressedData := bytes.NewReader(data)

			gzipReader, err := gzip.NewReader(compressedData)
			if err != nil {
				log.Fatal(err)
			}
			defer gzipReader.Close()
			// Print the size of the received data
			fmt.Printf("Received Data Size: %d bytes\n", len(data))
			var jsonDataBytes bytes.Buffer
			_, err = jsonDataBytes.ReadFrom(gzipReader)
			if err != nil {
				log.Fatal(err)
			}

			var jsonMessage map[string]interface{}
			err = json.Unmarshal(jsonDataBytes.Bytes(), &jsonMessage)
			if err != nil {
				log.Fatal(err)
			}

			elapsedTime := time.Since(startTime)
			log.Printf("Unmarshaling took %.3f ms\n", float64(elapsedTime.Milliseconds()))

			fmt.Println("Received Gzipped JSON Message:")
			//fmt.Println(jsonMessage)

			msg.Accept()
		}

	}
}
