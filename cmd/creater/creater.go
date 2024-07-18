package main

import (
	"bufio"
	"math/rand/v2"
	"merge-sort/internal/tracing"
	"os"
	"slices"
	"strconv"
	"strings"
)

var predefinedStrings = [][]byte{
	[]byte("Lorem ipsum dolor sit amet"),
	[]byte("consectetur adipiscing elit"),
	[]byte("sed do eiusmod tempor incididunt"),
	[]byte("labore et dolore magna aliqua"),
	[]byte("Ut enim ad minim veniam"),
	[]byte("quis nostrud exercitation ullamco laboris"),
}

var separator = []byte(". ")
var newLine = []byte("\n")

type args struct {
	size   int
	output string
}

func main() {
	defer tracing.Duration(tracing.Track("main"))
	maxNumber := 1000
	minLineLength := 4
	maxLineLength := getMaxLineLength(maxNumber)
	args := getArgs()

	file, err := os.Create(args.output)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	buffer := bufio.NewWriter(file)
	defer buffer.Flush()

	len := len(predefinedStrings)
	totalWrittenBytes := 0
	for i := 0; totalWrittenBytes+maxLineLength+minLineLength < args.size; i++ {
		random := rand.IntN(maxNumber)
		n, err := buffer.WriteString(strconv.Itoa(random))
		if err != nil {
			panic(err)
		}
		totalWrittenBytes += n

		n, err = buffer.Write(separator)
		if err != nil {
			panic(err)
		}
		totalWrittenBytes += n

		n, err = buffer.Write(predefinedStrings[random%len])
		if err != nil {
			panic(err)
		}
		totalWrittenBytes += n

		n, err = buffer.Write(newLine)
		if err != nil {
			panic(err)
		}
		totalWrittenBytes += n
	}

	_, err = buffer.WriteString("1. " + strings.Repeat("A", args.size-totalWrittenBytes-3))
	if err != nil {
		panic(err)
	}
}

func getMaxLineLength(maxNumber int) int {
	n := 3
	for maxNumber >= 10 {
		n++
		maxNumber /= 10
	}

	maxStringLength := 0
	for _, v := range predefinedStrings {
		maxStringLength = max(maxStringLength, len(v))
	}

	return n + maxStringLength
}

func getArgs() args {
	var err error
	args := args{size: 1000, output: "output.txt"}

	idx := slices.IndexFunc(os.Args, func(s string) bool { return s == "--size" })
	if idx >= 0 {
		args.size, err = strconv.Atoi(os.Args[idx+1])
		if err != nil {
			panic(err)
		}
	}

	idx = slices.IndexFunc(os.Args, func(s string) bool { return s == "--output" })
	if idx >= 0 {
		args.size, err = strconv.Atoi(os.Args[idx+1])
		if err != nil {
			panic(err)
		}
	}

	return args
}
