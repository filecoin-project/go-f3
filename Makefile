all: test lint

test:
	go test ./...
.PHONY: test

lint:
	golangci-lint run ./...
.PHONY: lint