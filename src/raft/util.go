package raft

import (
	"log"
)

// Debugging
const Debug = true

func Printf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
