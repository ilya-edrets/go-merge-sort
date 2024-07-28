package chunk

import (
	"bufio"
	"io"
	"merge-sort/internal/helpers"
	"os"
	"strconv"
)

const (
	defaultBufSize = 1024 * 1024
)

type LineReader struct {
	file         *os.File
	bufReader    *bufio.Reader
	numberBuffer []byte
	readLine     func(*LineReader) (Line, error)
}

func NewLineReader(isRawChunk bool) *LineReader {
	readLine := readStringLine
	if isRawChunk {
		readLine = readRawLine
	}

	return &LineReader{numberBuffer: make([]byte, 4), readLine: readLine}
}

func (reader *LineReader) Open(filepath string) error {
	var err error
	reader.file, err = os.Open(filepath)
	if err != nil {
		return err
	}

	reader.bufReader = bufio.NewReaderSize(reader.file, defaultBufSize)

	return nil
}

func (reader *LineReader) Close() error {
	return reader.file.Close()
}

func (reader *LineReader) ReadLine() (Line, error) {
	return reader.readLine(reader)
}

func readStringLine(reader *LineReader) (Line, error) {
	str, err := reader.bufReader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, err
	}

	if len(str) == 0 {
		return nil, nil
	}

	// trim new line
	lastN := 1
	if str[len(str)-1] != '\n' {
		lastN = 0
	}

	return stringToLine(str[:len(str)-lastN])
}

func readRawLine(reader *LineReader) (Line, error) {
	n, err := helpers.ReadFull(reader.bufReader, reader.numberBuffer)
	if err != nil && err != io.EOF {
		return nil, err
	}

	if n < 4 {
		if err == io.EOF {
			return nil, nil
		}
	}

	result := make([]byte, helpers.ToInt(reader.numberBuffer))
	_, err = helpers.ReadFull(reader.bufReader, result)
	if err == io.EOF {
		err = nil
	}

	return result, err
}

func stringToLine(str []byte) (Line, error) {
	i := 0
	for ; i < len(str) && str[i] != '.'; i++ {
	}

	if i == len(str) {
		return nil, nil
	}

	part1 := str[:i]
	text := str[i:]

	number, err := strconv.Atoi(string(part1))
	if err != nil {
		return nil, err
	}

	result := make([]byte, 4+len(text))
	resultPt2 := result[4:]

	helpers.ToBytes(result, number)
	copy(resultPt2, text)

	return result, nil
}
