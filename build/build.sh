#!/bin/bash
root=$(dirname "${BASH_SOURCE[0]}")/..

go build -o $root/dist/creater $root/cmd/creater/creater.go