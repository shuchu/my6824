#!/usr/bin/env bash
#


echo "Remove wc.so and mr-out*"
rm wc.so
rm mr-out*

echo "Build the wc.so"
go build -race -buildmodel=plugin ../../mrapps/wc.go

echo "Run Coordinator"
go run -race mrcoordinator.go pg-*.txt

echo "The Coordinator is running now"
