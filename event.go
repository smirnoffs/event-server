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
	original   string
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
	var event Event
	event.original = stringEvent
	data := strings.Split(stringEvent, "|")

	switch {
	case len(data) == 1 || (len(data) == 2 && data[1] != "B"):
		return event, fmt.Errorf("event is incomplete %v, data: %#v", stringEvent, data)
	case len(data) > 4:
		return event, fmt.Errorf("event is too long %v", stringEvent)
	case len(data) == 2 && data[1] == "B":
		sequence, err := strconv.ParseUint(data[0], 10, 64)
		if err != nil {
			return event, fmt.Errorf("sequence type is not an integer %v", stringEvent)
		}
		event.sequence = sequence
		_, present := allowedEvents[data[1]]
		if present == false {
			return event, fmt.Errorf("event type %v is not supported", data[1])
		}
		event.msgType = data[1]
	case len(data) >= 3:
		sequence, err := strconv.ParseUint(data[0], 10, 64)
		if err != nil {
			return event, fmt.Errorf("sequence type is not an integer %v", stringEvent)
		}
		event.sequence = sequence
		_, present := allowedEvents[data[1]]
		if present == false {
			return event, fmt.Errorf("event type %v is not supported", data[1])
		}
		event.msgType = data[1]
		fromUser, err := strconv.ParseUint(data[2], 10, 64)
		if err != nil {
			return event, fmt.Errorf("fromUserId is not an integer %v", stringEvent)
		}
		event.fromUserId = fromUser
		if len(data) == 4 {
			toUser, err := strconv.ParseUint(data[3], 10, 64)
			if err != nil {
				return event, fmt.Errorf("toUserId is not an integer %v", stringEvent)

			}
			event.toUserId = toUser
		}

	}
	return event, nil
}
