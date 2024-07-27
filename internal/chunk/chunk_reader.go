package chunk

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"strconv"
)

const (
	defaultBufSize = 1024 * 1024
)

type ChunkReader struct {
	file         *os.File
	bufReader    *bufio.Reader
	numberBuffer []byte
	readLine     func(*ChunkReader) ([]byte, error)
}

func NewChunkReader(isRawChunk bool) *ChunkReader {
	readLine := readStringLine
	if isRawChunk {
		readLine = readRawLine
	}

	return &ChunkReader{numberBuffer: make([]byte, 4), readLine: readLine}
}

func (reader *ChunkReader) Open(filepath string) error {
	var err error
	reader.file, err = os.Open(filepath)
	if err != nil {
		return err
	}

	reader.bufReader = bufio.NewReaderSize(reader.file, defaultBufSize)

	return nil
}

func (reader *ChunkReader) Close() error {
	return reader.file.Close()
}

func (reader *ChunkReader) ReadLine() ([]byte, error) {
	return reader.readLine(reader)
}

func readStringLine(reader *ChunkReader) ([]byte, error) {
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

func readRawLine(reader *ChunkReader) ([]byte, error) {
	n, err := reader.bufReader.Read(reader.numberBuffer)
	if err != nil && err != io.EOF {
		return nil, err
	}

	if n < 4 {
		_, err := reader.bufReader.Read(reader.numberBuffer[n:])
		if err != nil && err != io.EOF {
			return nil, err
		}

		if err == io.EOF {
			return nil, nil
		}
	}

	length := int(int32(binary.BigEndian.Uint32(reader.numberBuffer)))

	if length > 200 {
		panic(length)
	}

	result := make([]byte, length)
	n, err = reader.bufReader.Read(result)
	if err == io.EOF {
		err = nil
	}

	if n < length {
		_, err = reader.bufReader.Read(result[n:])
		if err == io.EOF {
			err = nil
		}
	}

	return result, err
}

func stringToLine(line []byte) ([]byte, error) {
	i := 0
	for ; i < len(line) && line[i] != '.'; i++ {
	}

	if i == len(line) {
		return nil, nil
	}

	part1 := line[:i]
	text := line[i:]

	n, err := strconv.Atoi(string(part1))
	if err != nil {
		return nil, err
	}
	number := int32(n)

	result := make([]byte, 4+len(text))
	resultPt2 := result[4:]
	binary.BigEndian.PutUint32(result, uint32(number))
	copy(resultPt2, text)

	return result, nil
}
