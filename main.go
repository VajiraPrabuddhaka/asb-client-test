package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	//azlog "github.com/Azure/azure-sdk-for-go/sdk/azcore/log"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"

	"nhooyr.io/websocket"
)

const (
	retryDelay     = 5 * time.Second // Time to wait before retrying
	receiveTimeout = 2 * time.Minute // Timeout for message reception
)

func main() {
	// Print log output to stdout
	//azlog.SetListener(func(event azlog.Event, s string) {
	//	fmt.Printf("[%s] %s\n", event, s)
	//})
	//
	//azlog.SetEvents(
	//	azservicebus.EventConn,
	//	azservicebus.EventAuth,
	//	azservicebus.EventReceiver,
	//	azservicebus.EventSender,
	//	azservicebus.EventAdmin,
	//)

	connectionString := os.Getenv("CONNECTION_STRING")
	topicName := os.Getenv("TOPIC_NAME")
	subscriptionName := os.Getenv("SUBSCRIPTION_NAME")

	// Store sent messages with timestamps for verification
	sentMessages := make(map[string]time.Time)
	var mu sync.Mutex

	// WebSocket connection function
	newWebSocketConnFn := func(ctx context.Context, args azservicebus.NewWebSocketConnArgs) (net.Conn, error) {
		opts := &websocket.DialOptions{Subprotocols: []string{"amqp"}}
		wssConn, _, err := websocket.Dial(ctx, args.Host, opts)
		if err != nil {
			return nil, err
		}
		return websocket.NetConn(ctx, wssConn, websocket.MessageBinary), nil
	}

	// Start the publisher in a separate goroutine
	opts1 := &azservicebus.ClientOptions{
		NewWebSocketConn: newWebSocketConnFn,
	}
	go startPublisher(connectionString, topicName, opts1, &mu, sentMessages)

	// Start the receiver with reconnection logic
	opts2 := &azservicebus.ClientOptions{
		NewWebSocketConn: newWebSocketConnFn,
	}
	go startReceiverWithRetries(connectionString, topicName, subscriptionName, opts2, &mu, sentMessages)

	// Goroutine to check for unreceived messages
	go func() {
		for {
			mu.Lock()
			for msgID, timestamp := range sentMessages {
				if time.Since(timestamp) > 1*time.Minute {
					log.Printf("Message %s was not received within 1 minute\n", msgID)
					delete(sentMessages, msgID) // Clean up unreceived messages
				}
			}
			mu.Unlock()

			time.Sleep(30 * time.Second) // Check every 30 seconds
		}
	}()

	// Block main to keep the program running
	select {}
}

func startPublisher(connectionString, topicName string, clientOpts *azservicebus.ClientOptions, mu *sync.Mutex, sentMessages map[string]time.Time) {
	for {
		client, sender, err := createSenderClient(connectionString, topicName, clientOpts)
		if err != nil {
			log.Printf("Publisher: failed to create client or sender, retrying in %v: %v", retryDelay, err)
			time.Sleep(retryDelay)
			continue
		}

		// Send messages every minute
		for {
			msgID := fmt.Sprintf("msg-%d", time.Now().Unix())
			message := &azservicebus.Message{
				MessageID: &msgID,
				Body:      []byte("Test message"),
			}

			mu.Lock()
			err := sender.SendMessage(context.Background(), message, nil)
			if err != nil {
				log.Printf("Publisher: failed to send message, reconnecting: %v", err)
				break // Break out of the loop to reconnect the sender
			}

			sentMessages[msgID] = time.Now()
			mu.Unlock()

			log.Printf("Publisher: sent message: %s\n", msgID)
			time.Sleep(1 * time.Minute) // Wait before sending the next message
		}

		sender.Close(context.Background())
		client.Close(context.Background())
	}
}

func startReceiverWithRetries(connectionString, topicName, subscriptionName string, clientOpts *azservicebus.ClientOptions, mu *sync.Mutex, sentMessages map[string]time.Time) {
	for {
		// Create receiver client with retry logic
		client, receiver, err := createReceiverClient(connectionString, topicName, subscriptionName, clientOpts)
		if err != nil {
			log.Printf("Receiver: failed to create client or receiver, retrying in %v: %v", retryDelay, err)
			time.Sleep(retryDelay)
			continue
		}

		// Start receiving messages
		for {
			ctx, cancel := context.WithTimeout(context.Background(), receiveTimeout)
			defer cancel()

			messages, err := receiver.ReceiveMessages(ctx, 10, nil)
			if err != nil {
				log.Printf("Receiver: error receiving messages, reconnecting: %v", err)
				break // Break out of the loop to reconnect the receiver
			}

			mu.Lock()
			for _, msg := range messages {
				log.Printf("Receiver: received message: %s :%s\n", string(msg.Body), msg.MessageID)
				delete(sentMessages, msg.MessageID)

				// Complete the message to remove it from the queue
				if err := receiver.CompleteMessage(ctx, msg, nil); err != nil {
					log.Printf("Receiver: failed to complete message: %v", err)
				}
			}
			mu.Unlock()

			time.Sleep(1 * time.Second) // Brief pause before checking for more messages
		}

		receiver.Close(context.Background())
		client.Close(context.Background())
	}
}

// Helper function to create sender client and sender
func createSenderClient(connectionString, topicName string, clientOpts *azservicebus.ClientOptions) (*azservicebus.Client, *azservicebus.Sender, error) {
	client, err := azservicebus.NewClientFromConnectionString(connectionString, clientOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create client: %v", err)
	}

	sender, err := client.NewSender(topicName, nil)
	if err != nil {
		client.Close(context.Background())
		return nil, nil, fmt.Errorf("failed to create sender: %v", err)
	}

	return client, sender, nil
}

// Helper function to create receiver client and receiver
func createReceiverClient(connectionString, topicName, subscriptionName string, clientOpts *azservicebus.ClientOptions) (*azservicebus.Client, *azservicebus.Receiver, error) {
	client, err := azservicebus.NewClientFromConnectionString(connectionString, clientOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create client: %v", err)
	}

	receiver, err := client.NewReceiverForSubscription(topicName, subscriptionName, nil)
	if err != nil {
		client.Close(context.Background())
		return nil, nil, fmt.Errorf("failed to create receiver: %v", err)
	}

	return client, receiver, nil
}
