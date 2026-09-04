.PHONY: test vet demo benchmark check

test:
	go test ./...

vet:
	go vet ./...

demo:
	go run ./cmd/kestrel-demo

benchmark:
	go test -run '^$$' -bench BenchmarkHealthyVerticalSlice -benchmem ./benchmarks

check: test vet
