package main

import (
	"fmt"
	"log"
)

func logF(node uint64, format string, args ...interface{}) {
	out := fmt.Sprintf("[%d] %s", node, format)
	log.Printf(out, args...)
}
