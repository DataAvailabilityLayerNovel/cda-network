# Stage 1: Build stage
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache git build-base

WORKDIR /app

# Copy all directories to compile Go workspace
COPY go.work go.work
COPY rlnc-rsmt2d/ rlnc-rsmt2d/
COPY cda-publisher-node/ cda-publisher-node/
COPY cda-bootstrap-node/ cda-bootstrap-node/
COPY cda-store-node/ cda-store-node/
COPY cda-light-node/ cda-light-node/

# Compile all binaries
RUN go build -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
RUN go build -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
RUN go build -o bin/store ./cda-store-node/cmd/store/main.go
RUN go build -o bin/light ./cda-light-node/cmd/light/main.go

# Stage 2: Runtime stage
FROM alpine:latest
RUN apk add --no-cache curl jq bash

WORKDIR /app

# Copy compiled binaries from builder
COPY --from=builder /app/bin/* /usr/local/bin/

# Default command can be overridden in docker-compose.yml
CMD ["/bin/sh"]
