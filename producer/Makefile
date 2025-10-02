.PHONY: proto clean

PROTOC := protoc
GEN_DIR := generated

# Generate Go code from protobuf definitions into ./generated
proto:
	$(PROTOC) --go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		proto/event/v1/event.proto

# Remove generated files
clean:
	$(RM) -r $(GEN_DIR)


