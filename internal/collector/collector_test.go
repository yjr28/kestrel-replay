package collector

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yjr28/kestrel-replay/internal/model"
)

func validEvent(id, trace string) model.Event {
	return model.Event{
		ID: id, Sequence: 1, Source: model.SourceApplication, Kind: model.KindSpan,
		TraceID: trace, SpanID: "span-1", Service: "inventory", Operation: "check",
		Timestamp: time.Now().UTC(), Status: "ok", Attributes: map[string]string{"duration_us": "10"},
	}
}

func postEvent(t *testing.T, url string, e model.Event) *http.Response {
	t.Helper()
	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url+"/v1/events", "application/json", bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestIngestQueryAndMetrics(t *testing.T) {
	c := New(Config{QueueCapacity: 8})
	defer c.Close()
	ts := httptest.NewServer(c.Handler())
	defer ts.Close()

	resp := postEvent(t, ts.URL, validEvent("e-1", "trace-a"))
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("post status=%d", resp.StatusCode)
	}
	resp.Body.Close()

	deadline := time.Now().Add(time.Second)
	for c.Stats().Stored != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if c.Stats().Stored != 1 {
		t.Fatalf("event not stored: %+v", c.Stats())
	}

	resp, err := http.Get(ts.URL + "/v1/events?trace_id=trace-a")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var got []model.Event
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != "e-1" {
		t.Fatalf("unexpected events: %+v", got)
	}

	resp, err = http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(raw), "kestrel_collector_events_stored_total 1") {
		t.Fatalf("metrics missing stored count: %s", raw)
	}
}

func TestRejectsInvalidEvent(t *testing.T) {
	c := New(Config{QueueCapacity: 1})
	defer c.Close()
	ts := httptest.NewServer(c.Handler())
	defer ts.Close()

	resp, err := http.Post(ts.URL+"/v1/events", "application/json", strings.NewReader(`{"id":"missing-fields"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if c.Stats().Invalid != 1 {
		t.Fatalf("invalid=%d", c.Stats().Invalid)
	}
}

func TestQueueOverloadIsExplicit(t *testing.T) {
	c := New(Config{QueueCapacity: 1, ProcessDelay: 100 * time.Millisecond})
	defer c.Close()
	ts := httptest.NewServer(c.Handler())
	defer ts.Close()

	statuses := make([]int, 0, 12)
	for i := 0; i < 12; i++ {
		e := validEvent("event-"+time.Now().Add(time.Duration(i)).Format("150405.000000000"), "trace-overload")
		resp := postEvent(t, ts.URL, e)
		statuses = append(statuses, resp.StatusCode)
		resp.Body.Close()
	}
	saw429 := false
	for _, code := range statuses {
		if code == http.StatusTooManyRequests {
			saw429 = true
		}
	}
	if !saw429 || c.Stats().Dropped == 0 {
		t.Fatalf("expected explicit overload; statuses=%v stats=%+v", statuses, c.Stats())
	}
}
