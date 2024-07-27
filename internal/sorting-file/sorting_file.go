package sortingfile

import (
	"merge-sort/internal/chunk"
	"path/filepath"
	"strconv"
)

type Line []byte

type SortingFile struct {
	filePath string
}

func NewSortingFile(filePath string) *SortingFile {
	return &SortingFile{filePath}
}

func (sortingFile *SortingFile) SplitIntoChunks(outputFolder string, chunkSize int64) ([]*chunk.Chunk, error) {
	result := make([]*chunk.Chunk, 0)

	reader := chunk.NewChunkReader(false)
	err := reader.Open(sortingFile.filePath)
	if err != nil {
		return result, err
	}
	defer reader.Close()

	var writer *chunk.ChunkWriter
	currentChunkSize := chunkSize
	chunkNumber := 0

	for line, err := reader.ReadLine(); line != nil; line, err = reader.ReadLine() {
		if err != nil {
			return nil, err
		}

		if currentChunkSize >= chunkSize {
			currentChunkSize = 0
			chunkNumber++
			filePath := filepath.Join(outputFolder, strconv.Itoa(chunkNumber)+".chunk")
			ch := chunk.NewChunk(filePath, true, true)
			writer = chunk.NewChunkWriter(true)
			err := writer.Create(filePath)
			if err != nil {
				return result, err
			}
			defer writer.Close()
			result = append(result, ch)
		}

		nn := 0
		nn, err = writer.WriteLine(line)
		if err != nil {
			return result, err
		}
		currentChunkSize += int64(nn)
	}

	return result, nil
}
