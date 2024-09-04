package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func main() {
	// Replace with your Service Bus connection string and topic/subscription details
	connectionString := "<connection_string>"
	topicName := "vj-test-100"
	subscriptionName := "vj-test-100-sub-1"

	// Create a Service Bus client
	client, err := azservicebus.NewClientFromConnectionString(connectionString, nil)
	if err != nil {
		log.Fatalf("Failed to create client: %s", err)
	}
	defer client.Close(context.Background())

	// Create a receiver for the topic subscription
	receiver, err := client.NewReceiverForSubscription(topicName, subscriptionName, nil)
	if err != nil {
		log.Fatalf("Failed to create receiver: %s", err)
	}
	defer receiver.Close(context.Background())

	fmt.Println("Listening for messages...")

	// Create a background context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Continuously receive messages
	for {
		// Receive messages from the topic subscription
		messages, err := receiver.ReceiveMessages(ctx, 10, nil)
		if err != nil {
			log.Printf("Failed to receive messages: %s", err)
			continue
		}

		for _, msg := range messages {
			// Print the message body
			fmt.Printf("Received message: %s\n", string(msg.Body))

			// Complete the message so it is removed from the queue
			err = receiver.CompleteMessage(ctx, msg, nil)
			if err != nil {
				log.Printf("Failed to complete message: %s", err)
			}
		}

		// Wait before checking for more messages
		time.Sleep(1 * time.Second)
	}
}
