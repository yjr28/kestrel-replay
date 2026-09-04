package collector

import (
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func TestStoreAssignsGlobalSequence(t *testing.T) {
	store := &Store{}
	for _, id := range []string{"a", "b"} {
		e := model.Event{ID: id, Source: model.SourceApplication, Kind: model.KindSpan, TraceID: "trace-a", SpanID: "span-" + id, Service: "gateway", Operation: "create_order", Timestamp: time.Now().UTC(), Status: "ok"}
		if err := store.Add(e); err != nil {
			t.Fatal(err)
		}
	}
	got := store.Snapshot("trace-a")
	if len(got) != 2 || got[0].Sequence != 1 || got[1].Sequence != 2 {
		t.Fatalf("unexpected snapshot: %+v", got)
	}
}
