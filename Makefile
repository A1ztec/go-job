.PHONY: build test test-race lint run up down fmt vet

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

up:
	docker compose up --build

down:
	docker compose down