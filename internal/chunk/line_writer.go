package chunk

import (
	"bufio"
	"merge-sort/internal/helpers"
	"os"
	"strconv"
)

type LineWriter struct {
	file         *os.File
	bufWriter    *bufio.Writer
	numberBuffer []byte
	writeLine    func(*LineWriter, Line) (int, error)
}

func NewLineWriter(isRawChunk bool) *LineWriter {
	writeLine := writeStringLine
	if isRawChunk {
		writeLine = writeRawLine
	}
	return &LineWriter{writeLine: writeLine, numberBuffer: make([]byte, 4)}
}

func (writer *LineWriter) Create(filepath string) error {
	var err error
	writer.file, err = os.Create(filepath)
	if err != nil {
		return err
	}

	writer.bufWriter = bufio.NewWriterSize(writer.file, defaultBufSize)

	return nil
}

func (reader *LineWriter) Close() error {
	err := reader.bufWriter.Flush()
	if err != nil {
		return err
	}

	return reader.file.Close()
}

func (writer *LineWriter) WriteLine(line Line) (int, error) {
	return writer.writeLine(writer, line)
}

func writeStringLine(writer *LineWriter, line Line) (int, error) {
	var n, total int
	var err error

	number := helpers.ToInt(line[:4])

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

func writeRawLine(writer *LineWriter, line Line) (int, error) {
	var n1, n2 int
	var err error

	helpers.ToBytes(writer.numberBuffer, len(line))
	n1, err = writer.bufWriter.Write(writer.numberBuffer)
	if err != nil {
		return n1, err
	}

	n2, err = writer.bufWriter.Write(line)

	return n1 + n2, err
}
