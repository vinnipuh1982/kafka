## Producer (Go)

Standard Go project layout for a Kafka producer application.

### Structure

```
cmd/producer        # app entrypoint
internal/app        # application orchestration (Run)
internal/config     # configuration loading
internal/codec      # protobuf serialization helpers
internal/kafka      # Kafka producer implementation
generated           # generated protobuf code (do not edit)
proto/              # protobuf definitions
```

### Prerequisites

- Go 1.22+
- protoc + protoc-gen-go, protoc-gen-go-grpc

### Setup

1. Initialize module (if not already):

   ```bash
   go mod tidy
   ```

2. Build and run:

   ```bash
   go run ./cmd/producer
   ```

### Protobuf

Generate Go code into `generated`:

```bash
make proto
```

### Configuration

Environment variables:

- `APP_ENV` (default: `development`)
- `APP_NAME` (default: `producer`)
- `KAFKA_BROKERS` (default: `localhost:9092`, comma-separated for multiple brokers)



