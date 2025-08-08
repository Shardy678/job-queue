# Start from the official Golang image
FROM golang:1.24.4-alpine AS builder

WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the Go app
RUN go build -o server ./cmd/server/main.go

# Use a minimal image for running
FROM alpine:latest
WORKDIR /app

# Copy the built binary from builder
COPY --from=builder /app/server ./server

# Copy any static/config files if needed
# COPY --from=builder /app/config.yaml ./config.yaml

EXPOSE 8080

CMD ["./server"]
