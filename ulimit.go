package main

import (
	"log"
	"syscall"
)

// setUlimit increases the maximum number of open file descriptors for the current process
func setUlimit() {
	// Attempt to set the resource limit for the number of open file descriptors
	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &syscall.Rlimit{
		Cur: 100_000,
		Max: 100_000,
	})
	if err != nil {
		log.Panicln("Failed to set ulimit:", err)
	}
}
