.PHONY: build clean install test run help

# Binary name
BINARY_NAME=pg-outbox-dispatcher
BUILD_DIR=bin

# Version info
VERSION ?= dev
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")

# Go build flags
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.buildTime=$(BUILD_TIME) -X main.gitCommit=$(GIT_COMMIT)"

## help: Show this help message
help:
	@echo 'Usage:'
	@echo '  make build       Build the dispatcher binary'
	@echo '  make clean       Remove build artifacts'
	@echo '  make install     Install the binary to GOPATH/bin'
	@echo '  make test        Run tests'
	@echo '  make run         Run the dispatcher with example config'
	@echo '  make docker      Build Docker image'
	@echo ''

## build: Build the dispatcher binary
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	go build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/dispatcher
	@echo "✓ Binary built: $(BUILD_DIR)/$(BINARY_NAME)"

## clean: Remove build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@echo "✓ Cleaned"

## install: Install the binary to GOPATH/bin
install: build
	@echo "Installing to GOPATH/bin..."
	go install $(LDFLAGS) ./cmd/dispatcher
	@echo "✓ Installed"

## test: Run tests
test:
	@echo "Running tests..."
	go test -v -race -cover ./...

## run: Run the dispatcher with example config
run: build
	@echo "Running $(BINARY_NAME)..."
	./$(BUILD_DIR)/$(BINARY_NAME) -config examples/config.example.yaml

## docker: Build Docker image
docker:
	@echo "Building Docker image..."
	docker build -t pg-outbox-dispatcher:$(VERSION) .
	@echo "✓ Docker image built: pg-outbox-dispatcher:$(VERSION)"

## mod: Download and tidy Go modules
mod:
	@echo "Downloading modules..."
	go mod download
	go mod tidy
	@echo "✓ Modules updated"

## fmt: Format Go code
fmt:
	@echo "Formatting code..."
	go fmt ./...
	@echo "✓ Code formatted"

## lint: Run golangci-lint
lint:
	@echo "Running linter..."
	golangci-lint run ./...

.DEFAULT_GOAL := help
