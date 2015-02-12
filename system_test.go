package forestbus

import (
	"sync"
	"testing"
	"time"
)

var TEST_CLUSTER_ID = "testcluster"
var TEST_NODES = []string{"localhost:3010", "localhost:3011", "localhost:3012"}
var TEST_TOPIC = "test1"

// Test basic send and get single message
func TestClientSendAndRecieve(t *testing.T) {
	client := NewClient(TEST_CLUSTER_ID, TEST_NODES)
	msg := time.Now().Format(time.StampMilli)
	msgs := make([][]byte, 1)
	msgs[0] = []byte(msg)
	indexes, err := client.SendMessages(TEST_TOPIC, msgs, true)
	if err != nil {
		t.Fatalf("Error sending messages: %v\n", err)
	}

	/*
		Wait for commit needs to be true, otherwise the get can fail as the message as the node used by the GetMessages call can be different to the one
		used by SendMessages.  In this scenario a message may have been committed on node 1 & 2, but the get needs to wait for it to reach node 3.
	*/
	getMsgs, nextID, err := client.GetMessages(TEST_TOPIC, indexes[0], 1, true)
	if len(getMsgs) == 0 {
		t.Fatalf("No messages retrieved.\n")
	}
	getMsgStr := string(getMsgs[0])
	if getMsgStr != msg {
		t.Errorf("Message retrieved (%v) doesn't match message sent (%v)\n", getMsgStr, msg)
	}

	if nextID != indexes[0]+1 {
		t.Errorf("Next ID was %v, expected %v\n", nextID, indexes[0]+1)
	}
}

// Test synchronous message batcher
func TestSynchronousMessageBatcher(t *testing.T) {
	client := NewClient(TEST_CLUSTER_ID, TEST_NODES)

	batcher := NewMessageBatcher(client, TEST_TOPIC)

	wg := sync.WaitGroup{}

	// Parallel goroutines can use the batcher, resulting in individual messages being batched together
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			// Send the message - if the internal buffer is full wait until it can be added.
			index, err := batcher.SendMessage([]byte("Message"), true)
			if err != nil {
				t.Errorf("Error sending message: %v\n", err)
			}
			if index == 0 {
				t.Errorf("Index for sent message was zero\n")
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

// Test synchronous message batcher
func TestSynchronousMessageBatcherNonBlocking(t *testing.T) {
	client := NewClient(TEST_CLUSTER_ID, TEST_NODES)

	batcher := NewMessageBatcher(client, TEST_TOPIC, MessageBatcherSetBufferSize(10))

	wg := sync.WaitGroup{}

	// Buffer size set to 10, so try sending 30 to ensure we hit a bottleneck
	passed := false
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			// Send the message - if the internal buffer is full wait until it can be added.
			index, err := batcher.SendMessage([]byte("Message"), false)
			if err != nil {
				if err == ErrMessageBatcherBufferFull {
					passed = true
				} else {
					t.Errorf("Error sending message: %v\n", err)
				}
			} else if index == 0 {
				t.Errorf("Index for sent message was zero\n")
			}
			wg.Done()
		}()
	}

	wg.Wait()

	if !passed {
		t.Errorf("Unable to trigger message buffer full\n")
	}
}

// Test asynchronous message batcher
func TestAsynchronousMessageBatcher(t *testing.T) {
	client := NewClient(TEST_CLUSTER_ID, TEST_NODES)

	batcher := NewMessageBatcher(client, TEST_TOPIC)

	responseChannel := make(chan *SendResult, 500)

	// Ensure that all messages from previous tests have had time to propagate
	time.Sleep(time.Second)

	nextIndexExpected, err := client.GetTopicMaxAvailableIndex(TEST_TOPIC)
	nextIndexExpected++

	if err != nil {
		t.Errorf("Error getting max available index: %v\n", err)
	}

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
			t.Errorf("Message %v encountered error: %v\n", *result.GetReference().(*int), result.GetError())
		}
		// Check that messages were processed in sequence
		if *result.GetReference().(*int) != responseCount {
			t.Errorf("Expected results in same sequence as sent, but saw message reference %v instead of %v\n", *result.GetReference().(*int), responseCount)
		}

		if string(result.GetMessage()) != "Message" {
			t.Errorf("Message on result was not Message: %v\n", result.GetMessage())
		}

		if result.GetIndex() != nextIndexExpected+int64(responseCount) {
			t.Errorf("Message index not in sequence as expected.  Result had %v, expected %v\n", result.GetIndex(), nextIndexExpected+int64(responseCount))
		}
	}
}
