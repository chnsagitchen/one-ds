package main

import (
	"github.com/chnsagitchen/one-ds/dsio"
	"fmt"
	"net"
	"bufio"
	"strings"
)

func handleDBCommand(command string, dsw *dsio.DSWALRecordWriter) string {
	commandArr := strings.Split(command, "|")
	record := &dsio.DSLogRecord {
		RecordKey: commandArr[1],
	}

	result := "ACK"
	if "GET" == commandArr[0] {
		result, _ = dsw.ReadRecord(record)
		fmt.Printf("result: %d", result)
	} else if "PUT" == commandArr[0] {
		record.RecordVal = commandArr[2]
		offset, _ := dsw.WriteRecord(record)
		fmt.Printf("offset: %d", offset)
	}

	return result
}

func handleDBConn(conn net.Conn, dsw *dsio.DSWALRecordWriter) {
	defer conn.Close()

	for {
		// will listen for message to process ending in newline (\n)
		command, _ := bufio.NewReader(conn).ReadString('#')
		// output message received
		fmt.Print("Command Received:", command)

		result := handleDBCommand(command[:len(command) - 1], dsw)
		conn.Write([]byte(result + "\n"))
	}
}

func main() {
	l, err := net.Listen("tcp", ":3130")
	if err != nil {
		fmt.Println("listen error:", err)
		return
	}

	dsw := dsio.New()

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			break
		}
		// start a new goroutine to handle
		// the new connection.
		go handleDBConn(c, dsw)
	}
}