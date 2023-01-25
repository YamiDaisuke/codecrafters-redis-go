package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// Parser

func readUntilTerminator(reader *bufio.Reader) ([]byte, error) {
	var last byte
	var bytes []byte
	for {
		byte, err := reader.ReadByte()
		if err != nil {
			last = 0
			return nil, err
		}
		bytes = append(bytes, byte)
		if last == '\r' && byte == '\n' {
			return bytes[:len(bytes)-2], nil
		}
		last = byte
	}
}

func readTerminator(reader *bufio.Reader) error {
	// Terminator should be \r\n
	last, err := reader.ReadByte()
	if err != nil {
		last = 0
		return err
	}
	if last != '\r' {
		return errors.New(fmt.Sprintf("unexpected input byte: %02x", last))
	}
	last, err = reader.ReadByte()
	if err != nil {
		last = 0
		return err
	}
	if last != '\n' {
		return errors.New(fmt.Sprintf("unexpected input byte: %02x", last))
	}

	return nil
}

func bulkStr(reader *bufio.Reader) (string, error) {
	var bytes []byte

	bytes, err := readUntilTerminator(reader)
	if err != nil {
		return "", err
	}

	length, err := strconv.Atoi(string(bytes))
	if err != nil {
		return "", err
	}

	// Handle null bulk string (length == -1)

	str, err := readUntilTerminator(reader)
	if err != nil {
		return "", err
	}

	if len(str) != int(length) {
		return "", errors.New("bulk string length does not match expected")
	}

	return string(str), nil
}

func array(reader *bufio.Reader) ([]interface{}, error) {
	var bytes []byte

	bytes, err := readUntilTerminator(reader)
	if err != nil {
		return nil, err
	}

	length, err := strconv.Atoi(string(bytes))
	if err != nil {
		return nil, err
	}

	output := make([]interface{}, int(length))
	for i := 0; i < int(length); i++ {
		next, err := readInput(reader)
		if err != nil {
			return nil, err
		}
		output[i] = next
	}

	return output, nil
}

func readInput(reader *bufio.Reader) (interface{}, error) {
	fmt.Println("Reading input...")
	dataType, err := reader.ReadByte()

	if err != nil {
		fmt.Println("Failed to read data type: ", err)
		return nil, err
	}

	fmt.Println("Reading...", string(dataType))
	switch dataType {
	case '+':
		return nil, errors.New("simple strings not implemented")
	case '-':
		return nil, errors.New("errors not implemented")
	case ':':
		return nil, errors.New("integers not implemented")
	case '$':
		return bulkStr(reader)
	case '*':
		return array(reader)
	default:
		return nil, errors.New(fmt.Sprintf("unsupported data type symbol: %d", dataType))
	}
}

// Commands

func executeCmd(cmd interface{}, conn net.Conn) {
	if cmdArr, ok := cmd.([]interface{}); ok {
		if cmdStr, ok := cmdArr[0].(string); ok {
			cmdStr = strings.ToUpper(cmdStr)
			fmt.Println("Executing: ", cmdStr)
			switch cmdStr {
			case "PING":
				conn.Write([]byte{'+', 'P', 'O', 'N', 'G', '\r', '\n'})
			}
		}
	}
}

// Server

func main() {
	PORT := "6379"
	fmt.Println("Starting redis server")
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", PORT))
	if err != nil {
		fmt.Println("Failed to bind to port: ", PORT)
		os.Exit(1)
	}

	fmt.Println("Server listening")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
		}

		reader := bufio.NewReader(conn)
		input, err := readInput(reader)

		if err != nil {
			fmt.Println("Error reading input: ", err.Error())
		}

		executeCmd(input, conn)
		conn.Close()
	}
}
