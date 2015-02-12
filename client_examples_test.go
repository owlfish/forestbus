package forestbus

import (
	"fmt"
)

func ExampleClient_GetMessages() {
	client := NewClient("test-cluster", []string{"localhost:3000,localhost:3001,localhost:3002"})

	msgs, nextID, err := client.GetMessages("test-topic", 1, 100, true)

	if err != nil {
		fmt.Printf("Error getting messages: %v\n", err)
		return
	}

	if len(msgs) == 0 && nextID == 0 {
		fmt.Printf("Commit index is zero due to a full cluster restart.)\n")
		return
	}

	fmt.Printf("Recieved %v messages with the next index being %v\n", len(msgs), nextID)

}

func ExampleClient_SendMessages() {
	client := NewClient("test-cluster", []string{"localhost:3000,localhost:3001,localhost:3002"})

	msgs := make([][]byte, 0)

	msgs = append(msgs, []byte("Message 1"))
	msgs = append(msgs, []byte("Message 2"))

	// Send the messages and wait for commit
	ids, err := client.SendMessages("test-topic", msgs, true)

	if err != nil {
		fmt.Printf("Error sending the messages: %v\n", err)
		return
	}

	if len(ids) == 2 {
		fmt.Printf("Message one at index %v, two at index %v\n", ids[0], ids[1])
	}
}
