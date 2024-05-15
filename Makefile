all: generate test lint

test: GOGC ?= 1000 # Reduce the GC frequency, default to 1000 if unset.
test:
	GOGC=$(GOGC) go test $(GOTEST_ARGS) ./...
.PHONY: test

test/cover: test
test/cover: GOTEST_ARGS=-coverprofile=coverage.txt -covermode=atomic -coverpkg=./...
.PHONY: test/cover


lint:
	golangci-lint run ./...
.PHONY: lint

generate:
	go generate ./...
.PHONY: generate
