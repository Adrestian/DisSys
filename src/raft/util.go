package raft

import "log"

// Debugging
const Debug = true

func Printf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return 0, nil
}

func Println(a ...interface{}) (n int, err error) {
	if Debug {
		log.Println(a...)
	}
	return 0, nil
}
