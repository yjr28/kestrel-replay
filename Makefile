BIN_DIR := .kestrel/bin
NODE_BIN := $(BIN_DIR)/kestrel-node

.PHONY: test vet integration build-demo demo demo-inprocess benchmark check clean

test:
	go test ./...

vet:
	go vet ./...

integration:
	go test ./integration -run TestMultiProcessFailureReplay -count=1 -v

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(NODE_BIN): | $(BIN_DIR)
	go build -o $@ ./cmd/kestrel-node

build-demo: $(NODE_BIN)

demo: build-demo
	go run ./cmd/kestrel-multiprocess-demo -node $(NODE_BIN)

demo-inprocess:
	go run ./cmd/kestrel-demo

benchmark:
	go test -run '^$$' -bench BenchmarkHealthyVerticalSlice -benchmem ./benchmarks

check: test vet

clean:
	rm -rf .kestrel
