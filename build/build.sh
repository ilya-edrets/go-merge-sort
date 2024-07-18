#!/bin/bash
root=$(dirname "${BASH_SOURCE[0]}")/..

go build -o $root/dist/creater $root/cmd/creater/creater.go
go build -o $root/dist/sorter $root/cmd/sorter/sorter.go