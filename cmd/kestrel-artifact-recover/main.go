package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/yjr28/kestrel-replay/internal/experiment"
)

func main() {
	root := flag.String("root", ".kestrel/experiments", "experiment root directory")
	id := flag.String("id", "", "experiment id to recover")
	staleAfter := flag.Duration("stale-after", 15*time.Minute, "minimum reservation age before dead-owner recovery")
	flag.Parse()

	if *id == "" {
		fmt.Fprintln(os.Stderr, "-id is required")
		os.Exit(2)
	}
	report, err := experiment.Recover(*root, *id, *staleAfter)
	if err != nil {
		fmt.Fprintf(os.Stderr, "recovery refused: %v\n", err)
		os.Exit(1)
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(report); err != nil {
		fmt.Fprintf(os.Stderr, "encode recovery report: %v\n", err)
		os.Exit(1)
	}
}
