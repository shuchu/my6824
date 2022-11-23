#!/usr/bin/env bash
#


echo "Remove wc.so and mr-out*"
rm wc.so
rm mr-out*
rm mr-*

echo "Build the wc.so"
go build -race -buildmode=plugin ../mrapps/wc.go

echo "Run Coordinator"
go run -race mrcoordinator.go pg-*.txt

