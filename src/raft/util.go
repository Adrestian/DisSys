package raft

import "log"

// Debugging
const Debug = false

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

const Debug2B = true

func Printf2B(format string, a ...interface{}) (n int, err error) {
	if Debug2B {
		log.Printf(format, a...)
	}
	return 0, nil
}
