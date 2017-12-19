package main

import (
	"net"
	"log"
	"fmt"
	"bufio"
	"strconv"
	"strings"
	"container/heap"
	"sync"
	"os"
)

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

const (
	host = "localhost"
)

var (
	eventPort  = getenv("EVENT_PORT", "9090")
	clientPort = getenv("CLIENT_PORT", "9099")

	clients               = make(map[uint64]net.Conn) // connected clients
	nextMessageSeq uint64 = 1
	followers             = make(map[uint64]map[uint64]struct{})

	queue         = make(sequenceQueue, 0)
	pushEventChan = make(chan Event)
	heapChanged   = make(chan bool)
	quitChan      = make(chan bool)

	mutex = &sync.Mutex{} // for safe modification of the heap with events
)

func main() {
	heap.Init(&queue)
	go runEventListener()
	// Close client connections at the end of the execution
	defer closeClientConnections()
	go runClientListener()
	go pushNewEventToQueue()
	go watchPriorityQueue()
	for {
		select {
		case <-quitChan:
			return
		}
	}
}

// Listen to incoming connections with events
func runEventListener() {
	eventListener, err := net.Listen("tcp", host+":"+eventPort)
	if err != nil {
		log.Fatalln("Cannot listen to events:", err.Error())
	}
	// close eventLister at the end of the execution
	defer eventListener.Close()

	eventConn, err := eventListener.Accept()
	if err != nil {
		log.Fatalln("Cannot accept an event connection:", err.Error())
	}
	log.Println("Ready to accept events")
	go handleEventRequest(eventConn)
}

// Handles incoming events
func handleEventRequest(conn net.Conn) {
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		// Builds the event.
		nextLine := scanner.Text()
		log.Println("Event received: ", nextLine)
		event, err := newEventFromString(nextLine)
		if err != nil {
			log.Println("Event error:", err)
		} else {
			pushEventChan <- event
		}
	}
}

// Watch for push to the priority queue,
// in case of the correct event sequence, pop the message to pop channel
func pushNewEventToQueue() {
	for {
		event := <-pushEventChan
		mutex.Lock()
		heap.Push(&queue, event)
		mutex.Unlock()
		heapChanged <- true
	}
}

// On each heap modification checks if the event with nextMessageSeq sequence number is in the top of the heap and
// sends the event to proceedEvent
func watchPriorityQueue() {
	for {
		if <-heapChanged {
			nextMessage := nextMessageSeq
			log.Println("Next to proceed:", nextMessage)
			if len(queue) > 0 && queue[0].sequence == nextMessage {
				mutex.Lock()
				event := heap.Pop(&queue).(Event)
				mutex.Unlock()
				go proceedEvent(event)
				if len(queue) > 0 {
					log.Printf("Submitted %d. Next in the queue: %d", event.sequence, queue[0].sequence)
				} else {
					log.Printf("Submitted %d. The queue is empty", event.sequence)
				}
			}
		}
	}
}

// Listen to incoming connections from clients
func runClientListener() {
	clientListener, err := net.Listen("tcp", host+":"+clientPort)
	if err != nil {
		log.Fatalln("Cannot listen to the client:", err.Error())
	}
	// clientListener should be closed at the end of the execution
	defer clientListener.Close()
	log.Println("Ready to listen to clients on port:" + clientPort)

	for {
		clientConn, err := clientListener.Accept()
		if err != nil {
			log.Fatalln("Error accepting the client connection:", err.Error())
		}
		go handleClientRequest(clientConn)
	}
}

// Increments nextMessageSeq and notifies heapChanged channel
func triggerNextHeapCheck(seq uint64) {
	nextMessageSeq = seq
	heapChanged <- true
}

// Dispatches an event
func proceedEvent(event Event) {
	defer triggerNextHeapCheck(event.sequence + 1)
	log.Printf("Proceeding event %s", event.toString())
	switch event.msgType {
	case "F": // Follow. Only the `To User Id` should be notified
		client, present := clients[event.toUserId]
		// In case if the event contains the id of the non-registered user
		if present == false {
			log.Println("Wrong subscription request, To User Id doesn't exist: ", event.toString())
		} else {
			sendMessage(client, event)
		}
		_, initialized := followers[event.toUserId]
		if initialized == false {
			followers[event.toUserId] = make(map[uint64]struct{})
		}
		followers[event.toUserId][event.fromUserId] = struct{}{} // empty structure is 0 bytes size
	case "U": // Unfollow. No clients should be notified
		delete(followers[event.toUserId], event.fromUserId)
	case "B": // Broadcast. All connected user clients should be notified
		for clientId := range clients {
			sendMessage(clients[clientId], event)
		}
	case "P": // Private message. Only the `To User Id` should be notified
		client, present := clients[event.toUserId]
		if present == false {
			log.Printf("Discharging the message to the non-registered client %d", event.toUserId)
		} else {
			sendMessage(client, event)
		}
	case "S": // Status update. All current followers of the `From User ID` should be notified
		//followers := getKeys(followers[event.fromUserId])
		for k := range followers[event.fromUserId] {
			client, present := clients[k]
			if present == false {
				log.Printf("There is no connection registered for subsriber %d. Event %s", k,
					event.toString())
			} else {
				sendMessage(client, event)
			}
		}
	}
}

// Sends the message to the opened connection
func sendMessage(connection net.Conn, event Event) {
	log.Printf("Event sent: %s", event.toString())
	connection.Write([]byte(event.toString() + "\n"))
}

// Handles client requests. Adds connection to a pool of active connections
func handleClientRequest(conn net.Conn) {
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("Error reading:", err)
	}

	strClientID := strings.Split(line, "\n")
	clientId, err := strconv.ParseUint(strClientID[0], 10, 64)
	if err != nil {
		conn.Write([]byte(fmt.Sprintf("Couldn't find a client id in %#v. Good buy!\n", line)))
		return
	}

	clients[clientId] = conn
	log.Println("New client connection. ClientID#", clientId)
}

// Used for closing opened connections from the clients at the end of the execution
func closeClientConnections() {
	for _, activeConnection := range clients {
		activeConnection.Close()
	}
}
