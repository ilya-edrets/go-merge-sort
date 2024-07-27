package main

import (
	"merge-sort/internal/chunk"
	sortingfile "merge-sort/internal/sorting-file"
	"merge-sort/internal/tracing"
	"os"
	"slices"
	"strconv"
)

type args struct {
	input        string
	outputFolder string
	outputFile   string
	chunkSize    int64
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
	chunk := mergeChunks(chunks)
	chunk.Rename(args.outputFile)
}

func splitIntoChunks(args args) []*chunk.Chunk {
	defer tracing.Duration(tracing.Track("splitIntoChunks"))

	inputFile := sortingfile.NewSortingFile(args.input)
	chunks, err := inputFile.SplitIntoChunks(args.outputFolder, args.chunkSize)
	if err != nil {
		panic(err)
	}

	return chunks
}

func sortChunks(chunks []*chunk.Chunk) {
	defer tracing.Duration(tracing.Track("sortChunks"))

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

func mergeChunks(chunks []*chunk.Chunk) *chunk.Chunk {
	defer tracing.Duration(tracing.Track("mergeChunks"))

	mergeChannel := make(chan mergeResult)

	for len(chunks) > 2 {
		var nextChunks []*chunk.Chunk

		for i := 0; i < len(chunks); i += 2 {
			if i+1 < len(chunks) {
				go func() {
					chunk, err := mergePair(chunks[i], chunks[i+1], true)
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

	chunk, err := mergePair(chunks[0], chunks[1], false)
	if err != nil {
		panic(err)
	}

	return chunk
}

func mergePair(chunk1 *chunk.Chunk, chunk2 *chunk.Chunk, isNewChunkRaw bool) (*chunk.Chunk, error) {
	defer tracing.Duration(tracing.Track("mergePair"))

	chunk, err := chunk1.Merge(chunk2, isNewChunkRaw)
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
		chunkSize:    350,
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
		var n int
		n, err = strconv.Atoi(os.Args[idx+1])
		if err != nil {
			panic(err)
		}
		args.chunkSize = int64(n)
	}

	fi, _ := os.Stat(args.input)
	if (fi.Size() / args.chunkSize) > 10 {
		args.chunkSize = fi.Size() / 10
	}

	return args
}
