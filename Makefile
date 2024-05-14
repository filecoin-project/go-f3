SHELL := /usr/bin/env bash

GOGC ?= 1000 # Reduce GC frequency during testing, default to 1000 if unset.

all: generate test fuzz lint

test:
	GOGC=$(GOGC) go test ./...
.PHONY: test

fuzz: FUZZTIME ?= 10s # The duration to run fuzz testing, default to 10s if unset.
fuzz: # List all fuzz tests across the repo, and run them one at a time with the configured fuzztime.
	@go run ./scripts/list_fuzz_tests | while read -r line; do \
		package=$$(echo $$line | cut -d: -f1); \
		func=$$(echo $$line | cut -d: -f2 | xargs); \
		echo "Running ./$$package $$func for $(FUZZTIME)..."; \
		GOGC=$(GOGC) go test ./$$package -fuzz=$$func -fuzztime=$(FUZZTIME); \
		echo "Done."; \
	done
.PHONY: fuzz

lint:
	go mod tidy
	golangci-lint run ./...
.PHONY: lint

generate:
	go generate ./...
.PHONY: generate