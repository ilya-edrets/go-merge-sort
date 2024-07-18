package main

import (
	"merge-sort/internal/chunk"
	"merge-sort/internal/task"
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

	inputFile := chunk.NewChunk(args.input)
	chunks, err := inputFile.SplitIntoChunks(args.outputFolder, args.chunkSize)
	if err != nil {
		panic(err)
	}

	tasks := make([]*task.Task[int], 0)
	for _, chunk := range chunks {
		tasks = append(tasks, task.Run(func() (int, error) { return 0, chunk.Sort() }))
	}

	for _, task := range tasks {
		_, err = task.Await()
		if err != nil {
			panic(err)
		}
	}

	channel := make(chan mergeResult)
	for len(chunks) > 1 {
		var nextChunks []*chunk.Chunk

		for i := 0; i < len(chunks); i += 2 {
			if i+1 < len(chunks) {
				mergePair(channel, chunks[i], chunks[i+1])
			} else {
				nextChunks = append(nextChunks, chunks[i])
			}
		}

		for i := 0; i < len(chunks)/2; i++ {
			mergeResult := <-channel
			if mergeResult.err != nil {
				panic(err)
			}
			nextChunks = append(nextChunks, mergeResult.chunk)
		}

		chunks = nextChunks
	}

	chunks[0].Rename(args.outputFile)
}

func mergePair(channel chan mergeResult, chunk1 *chunk.Chunk, chunk2 *chunk.Chunk) {
	go func() {
		chunk, err := chunk2.Merge(chunk1)
		if err != nil {
			channel <- mergeResult{chunk: chunk, err: err}
		}

		err = chunk1.Remove()
		if err != nil {
			channel <- mergeResult{chunk: chunk, err: err}
		}

		err = chunk2.Remove()
		channel <- mergeResult{chunk: chunk, err: err}
	}()
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
