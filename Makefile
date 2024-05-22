SHELL := /usr/bin/env bash

GOGC ?= 1000 # Reduce GC frequency during testing, default to 1000 if unset.

all: generate test fuzz lint

test:
	GOGC=$(GOGC) go test $(GOTEST_ARGS) ./...
.PHONY: test

test/cover: test
test/cover: GOTEST_ARGS=-coverprofile=coverage.txt -covermode=atomic -coverpkg=./...
.PHONY: test/cover

fuzz: FUZZTIME ?= 10s # The duration to run fuzz testing, default to 10s if unset.
fuzz: # List all fuzz tests across the repo, and run them one at a time with the configured fuzztime.
	@set -e; \
	go list ./... | while read -r package; do \
		go test -list '^Fuzz' "$$package" | grep '^Fuzz' | while read -r func; do \
			echo "Running $$package $$func for $(FUZZTIME)..."; \
			GOGC=$(GOGC) go test "$$package" -run '^$$' -fuzz="$$func" -fuzztime=$(FUZZTIME) || exit 1; \
		done; \
	done;
.PHONY: fuzz

lint:
	go mod tidy
	golangci-lint run ./...
.PHONY: lint

generate:
	go generate ./...
.PHONY: generate
