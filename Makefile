all: generate test lint

test:
	go test ./...
.PHONY: test

lint:
	golangci-lint run ./...
.PHONY: lint

generate:
	go generate ./...
.PHONY: generate