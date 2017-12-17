package main

import "net"

type clientConnection struct {
	connection       net.Conn
	id               uint64
	followers        map[uint64]bool
	messageSeqToSend uint64
	messageSeqSent   uint64
}
