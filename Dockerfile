# Stage 1: Build stage
FROM golang:1.25-alpine AS builder

RUN apk add --no-cache git build-base

WORKDIR /app

# Copy all directories to compile Go workspace
COPY go.work go.work
RUN go work edit -dropuse ./cometbft

COPY rlnc-rsmt2d/ rlnc-rsmt2d/
COPY cda-publisher-node/ cda-publisher-node/
COPY cda-bootstrap-node/ cda-bootstrap-node/
COPY cda-store-node/ cda-store-node/
COPY cda-light-node/ cda-light-node/
COPY cda-p2p/ cda-p2p/

# Compile all binaries statically
ENV CGO_ENABLED=0
RUN go build -ldflags="-s -w" -o bin/publisher ./cda-publisher-node/cmd/publisher/main.go
RUN go build -ldflags="-s -w" -o bin/bootstrap ./cda-bootstrap-node/cmd/bootstrap/main.go
RUN go build -ldflags="-s -w" -o bin/store ./cda-store-node/cmd/store/main.go
RUN go build -ldflags="-s -w" -o bin/light ./cda-light-node/cmd/light/main.go

# Stage 2: Runtime stage
FROM alpine:latest
RUN apk add --no-cache curl jq bash

WORKDIR /app

# Copy compiled binaries from builder
COPY --from=builder /app/bin/* /usr/local/bin/

# Default command can be overridden in docker-compose.yml
CMD ["/bin/sh"]
