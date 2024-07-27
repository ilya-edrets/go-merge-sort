package chunk

import (
	"bufio"
	"encoding/binary"
	"os"
	"strconv"
)

type ChunkWriter struct {
	file         *os.File
	bufWriter    *bufio.Writer
	numberBuffer []byte
	writeLine    func(*ChunkWriter, []byte) (int, error)
}

func NewChunkWriter(isRawChunk bool) *ChunkWriter {
	writeLine := writeStringLine
	if isRawChunk {
		writeLine = writeRawLine
	}
	return &ChunkWriter{writeLine: writeLine, numberBuffer: make([]byte, 4)}
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
	return writer.writeLine(writer, line)
}

func writeStringLine(writer *ChunkWriter, line []byte) (int, error) {
	var n, total int
	var err error

	number := int(int32(binary.BigEndian.Uint32(line)))

	n, err = writer.bufWriter.Write([]byte(strconv.Itoa(number)))
	total += n
	if err != nil {
		return total, err
	}

	n, err = writer.bufWriter.Write(line[4:])
	total += n
	if err != nil {
		return total, err
	}

	err = writer.bufWriter.WriteByte('\n')
	total += 1

	return total, err
}

func writeRawLine(writer *ChunkWriter, line []byte) (int, error) {
	var n1, n2 int
	var err error

	if uint32(len(line)) > 200 {
		panic(line)
	}

	binary.BigEndian.PutUint32(writer.numberBuffer, uint32(len(line)))
	n1, err = writer.bufWriter.Write(writer.numberBuffer)
	if err != nil {
		return n1, err
	}

	n2, err = writer.bufWriter.Write(line)

	if n1+n2 > 200 {
		panic(line)
	}

	return n1 + n2, err
}
