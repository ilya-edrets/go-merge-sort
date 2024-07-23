package chunk

import (
	"encoding/binary"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Line []byte

type Chunk struct {
	filePath string
	lines    []Line
}

func NewChunk(filePath string) *Chunk {
	return &Chunk{filePath: filePath}
}

func (chunk *Chunk) load() error {
	reader := NewChunkReader()
	err := reader.Open(chunk.filePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	chunk.lines = make([]Line, 0)
	isEOF := false
	for text, err := reader.ReadLine(); !isEOF; text, err = reader.ReadLine() {
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

		chunk.lines = append(chunk.lines, line)
	}

	return nil
}

func (chunk *Chunk) flush() error {
	writer := NewChunkWriter()
	err := writer.Create(chunk.filePath)
	if err != nil {
		return err
	}
	defer writer.Close()

	for i := 0; i < len(chunk.lines); i++ {
		line := chunk.lines[i]

		err := writeLine(line, writer)
		if err != nil {
			return err
		}
	}

	chunk.lines = nil
	return nil
}

func (chunk *Chunk) Sort() error {
	err := chunk.load()
	if err != nil {
		return err
	}
	sort.Slice(chunk.lines, func(i, j int) bool { return less(chunk.lines[i], chunk.lines[j]) })

	return chunk.flush()
}

func (chunk1 *Chunk) Merge(chunk2 *Chunk) (*Chunk, error) {
	reader1 := NewChunkReader()
	err := reader1.Open(chunk1.filePath)
	if err != nil {
		return nil, err
	}
	defer reader1.Close()

	reader2 := NewChunkReader()
	err = reader2.Open(chunk2.filePath)
	if err != nil {
		return nil, err
	}
	defer reader2.Close()

	outputFileName := strings.Split(chunk1.filePath, ".")[0] + "+" + strings.Split(chunk2.filePath, ".")[0] + ".chunk"
	result := NewChunk(outputFileName)
	writer := NewChunkWriter()
	err = writer.Create(outputFileName)
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	var line1, line2, resultLine Line = nil, nil, nil
	reader1End, reader2End := false, false

	for !reader1End || !reader2End {
		if line1 == nil {
			text1, err := reader1.ReadLine()
			reader1End = err == io.EOF
			line1, err = stringToLine(text1)
			if err != nil {
				return nil, err
			}
		}

		if line2 == nil {
			text2, err := reader2.ReadLine()
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

func writeLine(line Line, writer *ChunkWriter) error {
	number := int32(binary.BigEndian.Uint32(line))

	_, err := writer.WriteLine([]byte(strconv.Itoa(int(number))))
	if err != nil {
		return err
	}

	_, err = writer.WriteLine(line[4:])
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
