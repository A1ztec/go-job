.PHONY: build test test-race lint run up down fmt vet buildc

build:
	go build -o bin/worker ./cmd/worker

run:
	go run ./cmd/worker

test:
	go test ./...

test-race:
	go test -race ./...

fmt:
	gofmt -w .

vet:
	go vet ./...

lint: fmt vet
	golangci-lint run

buildc:
	docker build .

up:
	docker compose up -d

down:
	docker compose down