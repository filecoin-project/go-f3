SHELL := /usr/bin/env bash

GOLANGCILINT = go run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.59.1

all: generate test fuzz lint

test: GOGC ?= 1000 # Reduce GC frequency during testing, default to 1000 if unset.
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
	$(GOLANGCILINT) run ./...
.PHONY: lint

generate:
	go generate ./...
.PHONY: generate

build: f3
.PHONY: build

f3:
	go build ./cmd/f3
.PHONY: f3

gen:
	go generate ./...
.PHONY: gen