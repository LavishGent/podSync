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

# Build all packages
build:
    go build ./...
