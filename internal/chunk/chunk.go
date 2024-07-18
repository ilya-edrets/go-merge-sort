package chunk

import (
	"bufio"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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

	scanner := bufio.NewScanner(file)

	lines := make([]Line, 0)
	for scanner.Scan() {
		line, err := stringToLine(scanner.Text())
		if err != nil {
			return err
		}

		if line == nil {
			continue
		}

		lines = append(lines, line)
	}

	err = scanner.Err()
	if err != nil {
		return err
	}

	sort.Slice(lines, func(i, j int) bool { return less(lines[i], lines[j]) })

	file.Close()
	output, err := os.Create(chunk.filePath)
	if err != nil {
		return err
	}
	defer output.Close()

	writer := bufio.NewWriter(output)
	defer writer.Flush()

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		str := lineToString(line)
		_, err := writer.WriteString(str)
		if err != nil {
			return err
		}

		_, err = writer.WriteRune('\n')
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
	scanner := bufio.NewScanner(file)

	var writeBuffer *bufio.Writer
	currentChunkSize := chunkSize
	chunkNumber := 0
	for scanner.Scan() {
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
			writeBuffer = bufio.NewWriter(chunkFile)
			defer writeBuffer.Flush()
		}

		nn, err := writeLine(scanner.Text(), writeBuffer)
		if err != nil {
			return result, err
		}
		currentChunkSize += nn
	}

	return result, scanner.Err()
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

	scanner1 := bufio.NewScanner(file1)
	scanner2 := bufio.NewScanner(file2)
	writer := bufio.NewWriter(outputFile)
	defer writer.Flush()

	var line1, line2, resultLine Line = nil, nil, nil
	scanner1End, scanner2End := false, false

	for !scanner1End || !scanner2End {
		if line1 == nil {
			scanner1End = !scanner1.Scan()
			if !scanner1End {
				line1, err = stringToLine(scanner1.Text())
				if err != nil {
					return nil, err
				}
			}
		}

		if line2 == nil {
			scanner2End = !scanner2.Scan()
			if !scanner2End {
				line2, err = stringToLine(scanner2.Text())
				if err != nil {
					return nil, err
				}
			}
		}

		if scanner1End && scanner2End {
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

		str := lineToString(resultLine)
		_, err := writer.WriteString(str)
		if err != nil {
			return nil, err
		}

		_, err = writer.WriteRune('\n')
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

func writeLine(line string, writeBuffer *bufio.Writer) (int, error) {
	n1, _ := writeBuffer.WriteString(line)
	n2, err := writeBuffer.WriteRune('\n')

	return n1 + n2, err
}

func stringToLine(str string) (Line, error) {
	parts := strings.Split(str, ".")
	if len(parts) != 2 {
		return nil, nil
	}

	n, err := strconv.Atoi(parts[0])
	if err != nil {
		return nil, err
	}
	number := int32(n)
	text := parts[1]

	line := make([]byte, 4+len(text))
	binary.BigEndian.PutUint32(line, uint32(number))
	for i := 4; i < len(line); i++ {
		line[i] = text[i-4]
	}

	return line, nil
}

func lineToString(line Line) string {
	number := int32(binary.BigEndian.Uint32(line))

	part1 := strconv.Itoa(int(number))
	part2 := string(line[4:])

	return strings.Join([]string{part1, part2}, ".")
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
