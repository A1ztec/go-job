# --- build stage ---
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/worker ./cmd/worker

# --- final stage ---
FROM alpine:3.20

RUN adduser -D -g '' appuser
USER appuser

COPY --from=builder /bin/worker /bin/worker

ENTRYPOINT ["/bin/worker"]