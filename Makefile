all: generate test lint

test: GOGC ?= 1000 # Reduce the GC frequency, default to 1000 if unset.
test:
	GOGC=$(GOGC) go test ./...
.PHONY: test

lint:
	golangci-lint run ./...
.PHONY: lint

generate:
	go generate ./...
.PHONY: generate