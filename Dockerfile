# Build stage
FROM golang:1.23-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary
ARG VERSION=dev
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags "-X main.version=${VERSION} -X main.buildTime=$(date -u '+%Y-%m-%d_%H:%M:%S') -X main.gitCommit=$(git rev-parse --short HEAD 2>/dev/null || echo 'unknown')" \
    -o pg-outbox-dispatcher \
    ./cmd/dispatcher

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 outbox && \
    adduser -D -u 1000 -G outbox outbox

WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/pg-outbox-dispatcher /app/dispatcher
COPY --from=builder /build/examples/config.example.yaml /app/config.yaml

# Change ownership
RUN chown -R outbox:outbox /app

# Switch to non-root user
USER outbox

# Expose metrics port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run the dispatcher
ENTRYPOINT ["/app/dispatcher"]
CMD ["-config", "/app/config.yaml"]
