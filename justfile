# PodSync justfile

# Run tests
test:
    go test ./...

# Run tests with race detector
race:
    go test -race ./...

# Run linter
lint:
    golangci-lint run ./...

# Generate protobuf code
proto:
    protoc --go_out=. --go-grpc_out=. proto/*.proto

# Build all packages
build:
    go build ./...
