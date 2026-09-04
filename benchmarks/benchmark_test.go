package benchmarks

import (
	"github.com/yjr28/kestrel-replay/internal/demo"
	"testing"
)

// This is a development microbenchmark, not a publishable throughput benchmark.
// BENCHMARKS.md defines the production benchmark methodology and reporting gate.
func BenchmarkHealthyVerticalSlice(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := demo.RunScenario(nil); err != nil {
			b.Fatal(err)
		}
	}
}
