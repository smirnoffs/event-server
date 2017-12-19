package main

import (
	"net"
	"testing"
	"bufio"
	"time"
	"log"
)

func TestEventsDeliveredInOrder(t *testing.T) {
	go waitForTimeout()
	debug = true
	go runService()

	client, err := net.Dial("tcp", "localhost:9099")
	if err != nil {
		t.Error(err)
	}
	defer client.Close()
	client.Write([]byte("1\n"))

	eventSource, err := net.Dial("tcp", "localhost:9090")
	if err != nil {
		t.Error(err)
	}
	defer eventSource.Close()

	events := map[int]string{1: "1|F|1|2", 2: "2|S|2", 3: "3|B"}

	// Let's send messages in the reversed order
	eventSource.Write([]byte(events[3] + "\n" + events[2] + "\n" + events[1] + "\n"))

	scanner := bufio.NewScanner(client)

	// Only Status Update # 2 and Broadcast #3 has to be received
	expectedMessages := []int{2, 3}
	n := 0
	for scanner.Scan() {
		event := scanner.Text()
		if (event == events[expectedMessages[n]]) == false {
			log.Fatalf("expected event %s, received event %s", events[expectedMessages[n]], event)
		}
		n++
		if n > len(expectedMessages)-1 {
			break
		}
	}

}
func waitForTimeout() {
	timeoutTimer := time.NewTicker(time.Second)
	<-timeoutTimer.C
	log.Fatal("Timeed out")
}
