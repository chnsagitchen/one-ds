package main

import (
	"github.com/chnsagitchen/one-ds/dsio"
	"fmt"
)

func main() {
	record := dsio.DSLogRecord{
		"hello",
		"world",
	}
	dsw := dsio.New()
	offset, _ := dsw.WriteRecord(&record)
	fmt.Printf("offset: %d", offset)
	dsw.Stop()
}