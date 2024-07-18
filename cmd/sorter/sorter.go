package main

import (
	"merge-sort/internal/chunk"
	"merge-sort/internal/tracing"
	"os"
	"slices"
	"strconv"
)

type args struct {
	input        string
	outputFolder string
	outputFile   string
	chunkSize    int
}

type mergeResult struct {
	chunk *chunk.Chunk
	err   error
}

func main() {
	defer tracing.Duration(tracing.Track("main"))
	args := getArgs()

	chunks := splitIntoChunks(args)
	sortChunks(chunks)
	mergeChunks(chunks)
	chunks[0].Rename(args.outputFile)
}

func splitIntoChunks(args args) []*chunk.Chunk {
	inputFile := chunk.NewChunk(args.input)
	chunks, err := inputFile.SplitIntoChunks(args.outputFolder, args.chunkSize)
	if err != nil {
		panic(err)
	}

	return chunks
}

func sortChunks(chunks []*chunk.Chunk) {
	sortChannel := make(chan error)

	for _, chunk := range chunks {
		go func() {
			sortChannel <- chunk.Sort()
		}()
	}

	for i := 0; i < len(chunks); i++ {
		err := <-sortChannel
		if err != nil {
			panic(err)
		}
	}
}

func mergeChunks(chunks []*chunk.Chunk) {
	mergeChannel := make(chan mergeResult)

	for len(chunks) > 1 {
		var nextChunks []*chunk.Chunk

		for i := 0; i < len(chunks); i += 2 {
			if i+1 < len(chunks) {
				go func() {
					chunk, err := mergePair(chunks[i], chunks[i+1])
					mergeChannel <- mergeResult{chunk, err}
				}()
			} else {
				nextChunks = append(nextChunks, chunks[i])
			}
		}

		for i := 0; i < len(chunks)/2; i++ {
			mergeResult := <-mergeChannel
			if mergeResult.err != nil {
				panic(mergeResult.err)
			}
			nextChunks = append(nextChunks, mergeResult.chunk)
		}

		chunks = nextChunks
	}
}

func mergePair(chunk1 *chunk.Chunk, chunk2 *chunk.Chunk) (*chunk.Chunk, error) {
	chunk, err := chunk1.Merge(chunk2)
	if err != nil {
		return chunk, err
	}

	err = chunk1.Remove()
	if err != nil {
		return chunk, err
	}

	err = chunk2.Remove()
	return chunk, err
}

func getArgs() args {
	var err error
	args := args{
		input:        "unsorted.txt",
		outputFolder: ".",
		outputFile:   "sorted.txt",
		chunkSize:    1000,
	}

	idx := slices.IndexFunc(os.Args, func(s string) bool { return s == "--input" })
	if idx >= 0 {
		args.input = os.Args[idx+1]
	}

	idx = slices.IndexFunc(os.Args, func(s string) bool { return s == "--outputFolder" })
	if idx >= 0 {
		args.outputFolder = os.Args[idx+1]
	}

	idx = slices.IndexFunc(os.Args, func(s string) bool { return s == "--outputFile" })
	if idx >= 0 {
		args.outputFile = os.Args[idx+1]
	}

	idx = slices.IndexFunc(os.Args, func(s string) bool { return s == "--chunkSize" })
	if idx >= 0 {
		args.chunkSize, err = strconv.Atoi(os.Args[idx+1])
		if err != nil {
			panic(err)
		}
	}

	return args
}
