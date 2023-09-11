package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/interconnectedcloud/go-amqp"
)

type Foo struct {
	IntArray    []int     `json:"intArray"`
	FloatArray  []float32 `json:"floatArray"`
	StringArray []string  `json:"stringarray"`
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

func generateRandomString(length int) string {
	rand.Seed(time.Now().UnixNano())

	// Set of characters to choose from
	chars := "abcdefghijklmnopqrstuvwxyz"

	// Generate the random string
	result := make([]byte, length)
	for i := 0; i < length; i++ {
		result[i] = chars[rand.Intn(len(chars))]
	}

	return string(result)
}

func main() {
	masterBroker := "amqp://10.37.129.2:61616"
	username := "admin"
	password := "admin"
	queueName := "test/java"
	arraySize := 1000

	var client *amqp.Client
	var err error

	for {

		client, err = connectToBroker(masterBroker, username, password)

		if err != nil {
			// retry
			time.Sleep(5 * time.Second)
			continue
		}

		defer client.Close()

		senderSession, err := client.NewSession()
		if err != nil {
			log.Fatal("Creating session:", err)
		}
		defer senderSession.Close(nil)

		sender, err := senderSession.NewSender(
			amqp.LinkTargetAddress(queueName),
		)
		if err != nil {
			log.Fatal("Creating sender:", err)
		}
		defer sender.Close(nil)

		ctx := context.Background() // Create a context

		for {
			// Create a JSON representation of  data
			jsonMessage := map[string]interface{}{
				"intArray":    make([]int32, arraySize),
				"floatArray":  make([]float32, arraySize),
				"stringArray": make([]string, arraySize),
			}

			// Fill the int  with values from 1 to 10000
			for i := 0; i < arraySize; i++ {
				jsonMessage["intArray"].([]int32)[i] = int32(i + 1)
			}

			// Fill the float  with values from 1 to 10000
			for i := 0; i < arraySize; i++ {
				jsonMessage["floatArray"].([]float32)[i] = float32(i + 1)
			}

			// Fill the string  with values from 1 to 10000
			rand.Seed(time.Now().UnixNano())
			for i := 0; i < arraySize; i++ {
				// Generate a random string of length 10
				randomString := generateRandomString(10)
				jsonMessage["stringArray"].([]string)[i] = randomString
			}

			//startTime := time.Now() // Start the timer

			// Convert Foo instance to JSON
			jsonData, err := json.Marshal(jsonMessage)
			if err != nil {
				log.Fatal("Error marshaling JSON:", err)
			}

			// Send JSON data using the context
			err = sender.Send(ctx, amqp.NewMessage(jsonData))
			if err != nil {
				log.Fatal("Error sending message:", err)
			}

			//elapsedTime := time.Since(startTime)
			//log.Printf("marshaling took %.3f ms\n", float64(elapsedTime.Milliseconds()))

			log.Println("Sent JSON Message:")
			//log.Println(string(jsonData))

			time.Sleep(time.Second)
		}
	}
}
