package main

import (
	"strings"
	"strconv"
	"fmt"
)

type Event struct {
	sequence   uint64
	msgType    string
	fromUserId uint64
	toUserId   uint64
}

type Empty struct {
}

// Hash table with allowed events. Values cannot be nil, but can be struct, which is for cheap
var allowedEvents = map[string]Empty{
	"F": {}, // Follow
	"U": {}, // Unfollow
	"B": {}, // Broadcast
	"P": {}, // Private message
	"S": {}, // Status Update
}

func newEventFromString(stringEvent string) (Event, error) {
	var message Event
	data := strings.Split(stringEvent, "|")

	switch {
	case len(data) == 1 || len(data) == 2:
		return message, fmt.Errorf("event is incomplete %v", stringEvent)
	case len(data) > 4:
		return message, fmt.Errorf("event is too long %v", stringEvent)
	case len(data) >= 3:
		sequence, err := strconv.ParseUint(data[0], 10, 64)
		if err != nil {
			return message, fmt.Errorf("sequence type is not an integer %v", stringEvent)
		}
		message.sequence = sequence
		_, present := allowedEvents[data[1]]
		if present == false {
			return message, fmt.Errorf("event type %v is not supported", data[1])
		}
		message.msgType = data[1]
		fromUser, err := strconv.ParseUint(data[2], 10, 64)
		if err != nil {
			return message, fmt.Errorf("fromUserId is not an integer %v", stringEvent)
		}
		message.fromUserId = fromUser
		if len(data) == 4 {
			toUser, err := strconv.ParseUint(data[3], 10, 64)
			if err != nil {
				return message, fmt.Errorf("toUserId is not an integer %v", stringEvent)

			}
			message.toUserId = toUser
		}

	}
	return message, nil
}

func (e Event) toString() string {
	switch e.msgType {
	case "B": // Broadcast
		return fmt.Sprintf("%d|%s", e.sequence, e.msgType)
	case "S": //Status update
		return fmt.Sprintf("%d|%s|%d", e.sequence, e.msgType, e.fromUserId)
	case "F", "U", "P":
		return fmt.Sprintf("%d|%s|%d|%d", e.sequence, e.msgType, e.fromUserId, e.toUserId)
	}
	return "" // Just a fall back. We should never reach this point because the value of the msgType is checked already
}
