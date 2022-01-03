package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

func Printf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(nums ...int) int {
	if len(nums) == 0 {
		log.Fatalf("[Error]: in min(nums ...int), Zero Arguments\n")
	}
	var minimum = nums[0]
	for _, num := range nums {
		if num < minimum {
			minimum = num
		}
	}
	return minimum
}

func max(nums ...int) int {
	if len(nums) == 0 {
		log.Fatalf("[Error]: in max(nums ...int), Zero Arguments\n")
	}
	var maximum = nums[0]
	for _, num := range nums {
		if num > maximum {
			maximum = num
		}
	}
	return maximum
}

// stolen from https://stackoverflow.com/questions/55093676/checking-if-current-time-is-in-a-given-interval-golang/55093788
func inTimeSpan(start, end, check time.Time) bool {
	if start.Before(end) {
		return !check.Before(start) && !check.After(end)
	}
	if start.Equal(end) {
		return check.Equal(start)
	}
	panic("[Error]: inTimeSpan() start after end!")
}

func GetRandomTimeout(lo, hi int, unit time.Duration) time.Duration {
	if hi-lo < 0 || lo < 0 || hi < 0 {
		panic("[WARNING]: getRandomTimeout: Param Error")
	}
	return time.Duration(lo+rand.Intn(hi-lo)) * unit
}
