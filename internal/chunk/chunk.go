package chunk

import (
	"os"
	"sort"
	"strings"
)

type Line []byte

type Chunk struct {
	filePath    string
	isRawReader bool
	isRawWriter bool
	lines       []Line
}

func NewChunk(filePath string, isRawReader bool, isRawWriter bool) *Chunk {
	return &Chunk{filePath: filePath, isRawReader: isRawReader, isRawWriter: isRawWriter}
}

func (chunk *Chunk) load() error {
	reader := NewChunkReader(chunk.isRawReader)
	err := reader.Open(chunk.filePath)
	if err != nil {
		return err
	}
	defer reader.Close()

	chunk.lines = make([]Line, 0)
	for line, err := reader.ReadLine(); line != nil; line, err = reader.ReadLine() {
		if err != nil {
			return err
		}

		chunk.lines = append(chunk.lines, line)
	}

	return nil
}

func (chunk *Chunk) flush() error {
	writer := NewChunkWriter(chunk.isRawWriter)
	err := writer.Create(chunk.filePath)
	if err != nil {
		return err
	}
	defer writer.Close()

	for i := 0; i < len(chunk.lines); i++ {
		line := chunk.lines[i]

		_, err := writer.WriteLine(line)
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

func (chunk1 *Chunk) Merge(chunk2 *Chunk, isNewChunkRaw bool) (*Chunk, error) {
	reader1 := NewChunkReader(chunk1.isRawReader)
	err := reader1.Open(chunk1.filePath)
	if err != nil {
		return nil, err
	}
	defer reader1.Close()

	reader2 := NewChunkReader(chunk2.isRawWriter)
	err = reader2.Open(chunk2.filePath)
	if err != nil {
		return nil, err
	}
	defer reader2.Close()

	outputFileName := strings.Split(chunk1.filePath, ".")[0] + "+" + strings.Split(chunk2.filePath, ".")[0] + ".chunk"
	result := NewChunk(outputFileName, isNewChunkRaw, isNewChunkRaw)
	writer := NewChunkWriter(result.isRawWriter)
	err = writer.Create(outputFileName)
	if err != nil {
		return nil, err
	}
	defer writer.Close()

	var line1, line2, resultLine []byte = nil, nil, nil

	for {
		if line1 == nil {
			line1, err = reader1.ReadLine()
			if err != nil {
				return nil, err
			}
		}

		if line2 == nil {
			line2, err = reader2.ReadLine()
			if err != nil {
				return nil, err
			}
		}

		if line1 == nil && line2 == nil {
			break
		}

		if less(line1, line2) {
			resultLine = line1
			line1 = nil
		} else {
			resultLine = line2
			line2 = nil
		}

		_, err := writer.WriteLine(resultLine)
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
