.PHONY: proto clean mocks gen-mocks

PROTOC := protoc
GEN_DIR := generated
CURR_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
UID =
GID =

# Generate Go code from protobuf definitions into ./generated
proto:
	$(PROTOC) --go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		proto/event/v1/event.proto

# Remove generated files
clean:
	$(RM) -r $(GEN_DIR)

mocks gen-mocks:
	@echo Generating mocks with mockery (docker)
	docker run --rm -v "${CURR_DIR}":/src -w /src vektra/mockery:v2.46.3 \
		--name IProducerKafka --dir internal/kafka --output internal/mocks --outpkg mocks --with-expecter