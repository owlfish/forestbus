package forestbus

import (
	"fmt"
	"sync"
)

func ExampleMessageBatcher_SendMessage() {
	client := NewClient("test-cluster", []string{"localhost:3000,localhost:3001,localhost:3002"})

	batcher := NewMessageBatcher(client, "test-topic")

	wg := sync.WaitGroup{}

	// Parallel goroutines can use the batcher, resulting in individual messages being batched together
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			// Send the message - if the internal buffer is full wait until it can be added.
			index, err := batcher.SendMessage([]byte("Message"), true)
			if err != nil {
				fmt.Printf("Error sending message: %v\n", err)
			} else {
				fmt.Printf("Message index %v returned\n", index)
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func ExampleMessageBatcher_AsyncSendMessage() {
	client := NewClient("test-cluster", []string{"localhost:3000,localhost:3001,localhost:3002"})

	batcher := NewMessageBatcher(client, "test-topic")

	responseChannel := make(chan *SendResult, 500)

	go func() {
		for i := 0; i < 1000; i++ {
			// Need to use a local copy if i so we can pass it's address as the reference
			localID := i
			// Send messages, blocking if the buffer is full.
			// There is no need to check the error response in this instance.
			batcher.AsyncSendMessage([]byte("Message"), &localID, responseChannel, true)
		}
	}()

	for responseCount := 0; responseCount < 1000; responseCount++ {
		result := <-responseChannel
		if result.GetError() != nil {
			fmt.Printf("Message %v encountered error: %v\n", *result.GetReference().(*int), result.GetError())
		} else {
			fmt.Printf("Message %v given index %v\n", *result.GetReference().(*int), result.GetIndex())
		}
	}
}
