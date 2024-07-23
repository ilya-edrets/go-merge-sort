package chunk

import (
	"bufio"
	"os"
)

type ChunkWriter struct {
	file      *os.File
	bufWriter *bufio.Writer
}

func NewChunkWriter() *ChunkWriter {
	return &ChunkWriter{}
}

func (writer *ChunkWriter) Create(filepath string) error {
	var err error
	writer.file, err = os.Create(filepath)
	if err != nil {
		return err
	}

	writer.bufWriter = bufio.NewWriterSize(writer.file, defaultBufSize)

	return nil
}

func (reader *ChunkWriter) Close() error {
	err := reader.bufWriter.Flush()
	if err != nil {
		return err
	}

	return reader.file.Close()
}

func (writer *ChunkWriter) WriteLine(line []byte) (int, error) {
	return writer.bufWriter.Write(line)
}

func (writer *ChunkWriter) WriteByte(c byte) error {
	return writer.bufWriter.WriteByte(c)
}
