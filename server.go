package main

import (
	"net"
	"log"
	"fmt"
	"bufio"
	"strconv"
	"strings"
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
	eventsBucket          = make(map[uint64]Event)
	followers             = make(map[uint64]map[uint64]struct{})

	pushEventChan = make(chan Event)
	bucketChanged = make(chan bool)
	quitChan      = make(chan bool)

	mutex = &sync.Mutex{} // for safe modification of the heap with events

	// newbie log level control
	debug = getenv("DEBUG", "false") == "true"
)

func main() {
	//heap.Init(&queue)
	go runEventListener()
	// Close client connections at the end of the execution
	defer closeClientConnections()
	go runClientListener()
	go newEventToBucket()
	go watchEventBucket()
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
		// Builds events
		nextLine := scanner.Text()
		if debug {
			log.Println("Event received: ", nextLine)
		}
		event, err := newEventFromString(nextLine)
		if err != nil {
			if debug {
				log.Println("Event error:", err)
			}
		} else {
			pushEventChan <- event
		}
	}
}

// Add an Event into the bucket and notify the channel that the bucket has been changed
func newEventToBucket() {
	for {
		event := <-pushEventChan
		mutex.Lock()
		eventsBucket[event.sequence] = event
		mutex.Unlock()
		bucketChanged <- true
	}
}

// On each heap modification checks if the event with nextMessageSeq sequence number is in the top of the heap and
// sends the event to proceedEvent
func watchEventBucket() {
	for {
		if <-bucketChanged {
			if debug {
				log.Println("Next to proceed:", nextMessageSeq)
			}
			mutex.Lock()
			_, present := eventsBucket[nextMessageSeq]
			mutex.Unlock()
			if present == true {
				mutex.Lock()
				event := eventsBucket[nextMessageSeq]
				delete(eventsBucket, nextMessageSeq)
				mutex.Unlock()
				go proceedEvent(event)

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

// Increments nextMessageSeq and notifies bucketChanged channel
func triggerNextHeapCheck(seq uint64) {
	nextMessageSeq = seq
	bucketChanged <- true
}

// Dispatches an event
func proceedEvent(event Event) {
	defer triggerNextHeapCheck(event.sequence + 1)
	if debug {
		log.Printf("Proceeding event %s", event.original)
	}
	switch event.msgType {
	case "F": // Follow. Only the `To User Id` should be notified
		client, present := clients[event.toUserId]
		// In case if the event contains the id of the non-registered user
		if present == false {
			if debug {
				log.Println("Wrong subscription request, To User Id doesn't exist: ", event.original)
			}
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
			if debug {
				log.Printf("Discharging the message to the non-registered client %d", event.toUserId)
			}
		} else {
			sendMessage(client, event)
		}
	case "S": // Status update. All current followers of the `From User ID` should be notified
		//followers := getKeys(followers[event.fromUserId])
		for k := range followers[event.fromUserId] {
			client, present := clients[k]
			if present == false {
				if debug {
					log.Printf("There is no connection registered for subsriber %d. Event %s", k,
						event.original)
				}
			} else {
				sendMessage(client, event)
			}
		}
	}
}

// Sends the message to the opened connection
func sendMessage(connection net.Conn, event Event) {
	if debug {
		log.Printf("Event sent: %s", event.original)
	}
	connection.Write([]byte(event.original + "\n"))
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
