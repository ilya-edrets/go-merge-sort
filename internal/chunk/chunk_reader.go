package chunk

import (
	"bufio"
	"os"
)

const (
	defaultBufSize = 1024 * 1024
)

type ChunkReader struct {
	file      *os.File
	bufReader *bufio.Reader
}

func NewChunkReader() *ChunkReader {
	return &ChunkReader{}
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
	return reader.bufReader.ReadBytes('\n')
}
