package main

import (
	"net"
	"log"
	"fmt"
	"bufio"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	host       = "localhost"
	eventPort  = "9090"
	clientPort = "9099"
)

var clients = make(map[uint64]clientConnection) // connected clients
var chanEvents = make(chan Event)
var sentSeq uint64 = 0 // to keep track the number of sent

func main() {
	go runEventListener()
	// Close client connections at the end of the execution
	defer closeClientConnections()
	go runClientListener()
	for {
		incomingEvent := <-chanEvents
		go proceedEvent(incomingEvent)
	}
}

// Decides what to do with the event
func proceedEvent(event Event) {
	for i := event.sequence; i <= sentSeq; {
		log.Println(sentSeq, event.sequence)
		switch event.msgType {
		case "F": // Follow. Only the `To User Id` should be notified
			client, present := clients[event.toUserId]
			// In case if the event contains the id of the non-registered user
			if present == false {
				log.Println("Wrong subscription request, To User Id doesn't exist: ", event.toString())
				return
			}
			client.followers[event.fromUserId] = true
			go sendMessage(client, event)
		case "U": // Unfollow. No clients should be notified
			unfollow, present := clients[event.toUserId]
			// In case if the event contains the id of the non-registered user
			if present == false {
				log.Println("Wrong unfollow request, To User Id doesn't exist: ", event.toString())
				return
			}
			delete(unfollow.followers, event.fromUserId)
			atomic.AddUint64(&sentSeq, 1)
		case "B": // Broadcast. All connected user clients should be notified
			for _, client := range clients {
				go sendMessage(client, event)
			}
		case "P": // Private message. Only the `To User Id` should be notified
			client, present := clients[event.toUserId]
			if present == false {
				log.Printf("Discharging the message to the non-registered client")
				return
			}
			go sendMessage(client, event)
		case "S": // Status update. All current followers of the `From User ID` should be notified
			followers := clients[event.fromUserId].followers
			for followerId := range followers {
				client, present := clients[followerId]
				if present == false {
					log.Fatalln("There is no connection registered for subsriber %d. Event %s", followerId,
						event.toString())
				}
				go sendMessage(client, event)
			}
		}
	}
}

func sendMessage(client clientConnection, event Event) {
	log.Printf("Current sequense for %d is %d. Message seq %d", client.id,
		client.messageSeqToSend, event.sequence)
	log.Printf("Sending event: %#v (%s)", event, event.toString())
	client.connection.Write([]byte(event.toString() + "\n"))
	atomic.AddUint64(&sentSeq, 1)
	return
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
			chanEvents <- event
		}
	}

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

	clientConn := clientConnection{connection: conn, id: clientId, followers: make(map[uint64]bool)}
	clients[clientId] = clientConn
	log.Println("New client connection. ClientID#", clientId)
}

// Used for closing opened connections from the clients at the end of the execution
func closeClientConnections() {
	for _, activeConnection := range clients {
		activeConnection.connection.Close()
	}
}
