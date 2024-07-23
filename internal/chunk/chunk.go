package chunk

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	defaultBufSize = 1024 * 1024
)

type Line []byte

type Chunk struct {
	filePath string
}

func NewChunk(filePath string) *Chunk {
	return &Chunk{filePath: filePath}
}

func (chunk *Chunk) Sort() error {
	file, err := os.Open(chunk.filePath)
	if err != nil {
		return err
	}

	defer file.Close()

	reader := bufio.NewReaderSize(file, defaultBufSize)

	lines := make([]Line, 0)
	isEOF := false
	for text, err := reader.ReadBytes('\n'); !isEOF; text, err = reader.ReadBytes('\n') {
		isEOF = err == io.EOF
		if err != nil && !isEOF {
			return err
		}

		line, err := stringToLine(text)
		if err != nil {
			return err
		}

		if line == nil {
			continue
		}

		lines = append(lines, line)
	}
	file.Close()

	sort.Slice(lines, func(i, j int) bool { return less(lines[i], lines[j]) })

	output, err := os.Create(chunk.filePath)
	if err != nil {
		return err
	}
	defer output.Close()

	writer := bufio.NewWriter(output)
	defer writer.Flush()

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		err := writeLine(line, writer)
		if err != nil {
			return err
		}
	}

	return nil
}

func (chunk *Chunk) SplitIntoChunks(outputFolder string, chunkSize int) ([]*Chunk, error) {
	result := make([]*Chunk, 0)

	file, err := os.Open(chunk.filePath)
	if err != nil {
		return result, err
	}
	defer file.Close()
	reader := bufio.NewReaderSize(file, 1024*1024)

	var writeBuffer *bufio.Writer
	currentChunkSize := chunkSize
	chunkNumber := 0
	for line, err := reader.ReadBytes('\n'); true; line, err = reader.ReadBytes('\n') {
		isEOF := err == io.EOF
		if err != nil && !isEOF {
			return result, err
		}

		if currentChunkSize >= chunkSize {
			currentChunkSize = 0
			chunkNumber++
			filePath := filepath.Join(outputFolder, strconv.Itoa(chunkNumber)+".chunk")
			chunk := NewChunk(filePath)
			chunkFile, err := os.Create(filePath)
			if err != nil {
				return result, err
			}
			defer file.Close()
			result = append(result, chunk)
			writeBuffer = bufio.NewWriterSize(chunkFile, 1024*1024)
			defer writeBuffer.Flush()
		}

		nn := 0
		nn, err = writeBuffer.Write(line)
		if err != nil {
			return result, err
		}
		currentChunkSize += nn
		if isEOF {
			if line[len(line)-1] != '\n' {
				writeBuffer.WriteByte('\n')
			}
			break
		}
	}

	return result, nil
}

func (chunk1 *Chunk) Merge(chunk2 *Chunk) (*Chunk, error) {
	file1, err := os.Open(chunk1.filePath)
	if err != nil {
		return nil, err
	}
	defer file1.Close()

	file2, err := os.Open(chunk2.filePath)
	if err != nil {
		return nil, err
	}
	defer file2.Close()

	outputFileName := strings.Split(chunk1.filePath, ".")[0] + "+" + strings.Split(chunk2.filePath, ".")[0] + ".chunk"
	result := NewChunk(outputFileName)
	outputFile, err := os.Create(outputFileName)
	if err != nil {
		return nil, err
	}
	defer outputFile.Close()

	reader1 := bufio.NewReaderSize(file1, defaultBufSize)
	reader2 := bufio.NewReaderSize(file2, defaultBufSize)
	writer := bufio.NewWriterSize(outputFile, defaultBufSize)
	defer writer.Flush()

	var line1, line2, resultLine Line = nil, nil, nil
	reader1End, reader2End := false, false

	for !reader1End || !reader2End {
		if line1 == nil {
			text1, err := reader1.ReadBytes('\n')
			reader1End = err == io.EOF
			line1, err = stringToLine(text1)
			if err != nil {
				return nil, err
			}
		}

		if line2 == nil {
			text2, err := reader2.ReadBytes('\n')
			reader2End = err == io.EOF
			line2, err = stringToLine(text2)
			if err != nil {
				return nil, err
			}
		}

		if reader1End && reader2End {
			break
		}

		if line1 == nil {
			resultLine = line2
			line2 = nil
		} else if line2 == nil {
			resultLine = line1
			line1 = nil
		} else if less(line1, line2) {
			resultLine = line1
			line1 = nil
		} else {
			resultLine = line2
			line2 = nil
		}

		err := writeLine(resultLine, writer)
		if err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (chunk *Chunk) Remove() error {
	return os.Remove(chunk.filePath)
}

func (chunk *Chunk) Rename(filePath string) error {
	err := os.Rename(chunk.filePath, filePath)
	if err == nil {
		chunk.filePath = filePath
	}

	return err
}

func stringToLine(line []byte) (Line, error) {
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

func writeLine(line Line, writer *bufio.Writer) error {
	number := int32(binary.BigEndian.Uint32(line))

	_, err := writer.WriteString(strconv.Itoa(int(number)))
	if err != nil {
		return err
	}

	_, err = writer.Write(line[4:])
	if err != nil {
		return err
	}

	return nil
}

func less(line1 Line, line2 Line) bool {
	if line1 == nil {
		return false
	}

	if line2 == nil {
		return true
	}

	for i := 0; i < len(line1) || i < len(line2); i++ {
		if line1[i] != line2[i] {
			return line1[i] < line2[i]
		}
	}

	return len(line1) < len(line2)
}
