BIN_DIR := .kestrel/bin
NODE_BIN := $(BIN_DIR)/kestrel-node
ARTIFACT_REPLAY_BIN := $(BIN_DIR)/kestrel-artifact-replay
ARTIFACT_RECOVER_BIN := $(BIN_DIR)/kestrel-artifact-recover
CORPUS_BIN := $(BIN_DIR)/kestrel-corpus

.PHONY: test vet integration corpus build-demo demo demo-inprocess artifact-replay artifact-recover benchmark check clean

test:
	go test ./...

vet:
	go vet ./...

integration:
	go test ./integration -count=1 -v

$(BIN_DIR):
	mkdir -p $(BIN_DIR)

$(NODE_BIN): | $(BIN_DIR)
	go build -o $@ ./cmd/kestrel-node

$(ARTIFACT_REPLAY_BIN): | $(BIN_DIR)
	go build -o $@ ./cmd/kestrel-artifact-replay

$(ARTIFACT_RECOVER_BIN): | $(BIN_DIR)
	go build -o $@ ./cmd/kestrel-artifact-recover

$(CORPUS_BIN): | $(BIN_DIR)
	go build -o $@ ./cmd/kestrel-corpus

build-demo: $(NODE_BIN)

demo: build-demo
	go run ./cmd/kestrel-multiprocess-demo -node $(NODE_BIN)

demo-inprocess:
	go run ./cmd/kestrel-demo

artifact-replay: $(NODE_BIN) $(ARTIFACT_REPLAY_BIN)
	@test -n "$(ARTIFACT)" || (echo "ARTIFACT=<experiment-directory> is required" >&2; exit 2)
	$(ARTIFACT_REPLAY_BIN) -artifact "$(ARTIFACT)" -node $(NODE_BIN)

artifact-recover: $(ARTIFACT_RECOVER_BIN)
	@test -n "$(EXPERIMENT)" || (echo "EXPERIMENT=<experiment-id> is required" >&2; exit 2)
	$(ARTIFACT_RECOVER_BIN) -root .kestrel/experiments -id "$(EXPERIMENT)" -stale-after "$(or $(STALE_AFTER),15m)"

corpus: $(NODE_BIN) $(ARTIFACT_REPLAY_BIN) $(CORPUS_BIN)
	$(CORPUS_BIN) -node $(NODE_BIN) -replay $(ARTIFACT_REPLAY_BIN) -root .kestrel/corpus-runs

benchmark:
	go test -run '^$$' -bench BenchmarkHealthyVerticalSlice -benchmem ./benchmarks

check: test vet

clean:
	rm -rf .kestrel
