package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/yjr28/kestrel-replay/internal/model"
)

type OutcomeSignature struct {
	Classification  string   `json:"classification"`
	HTTPStatus      int      `json:"http_status"`
	TerminalService string   `json:"terminal_service,omitempty"`
	ErrorCode       string   `json:"error_code,omitempty"`
	CausalPath      []string `json:"causal_path,omitempty"`
}

func (o OutcomeSignature) Digest() string {
	h := sha256.New()
	fmt.Fprintf(h, "%s\n%d\n%s\n%s\n", o.Classification, o.HTTPStatus, o.TerminalService, o.ErrorCode)
	for _, p := range o.CausalPath {
		fmt.Fprintln(h, p)
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func Equivalent(a, b OutcomeSignature) bool {
	if a.Classification != b.Classification || a.HTTPStatus != b.HTTPStatus || a.TerminalService != b.TerminalService || a.ErrorCode != b.ErrorCode {
		return false
	}
	if len(a.CausalPath) != len(b.CausalPath) {
		return false
	}
	for i := range a.CausalPath {
		if a.CausalPath[i] != b.CausalPath[i] {
			return false
		}
	}
	return true
}

// MessageDeliverySignature intentionally ignores generated message/span IDs and
// timing. It captures the observable asynchronous side-effect shape for one
// topic: how many application publish events occurred and how many consumes
// each service recorded. This lets replay distinguish a duplicated delivery
// from an otherwise identical successful HTTP outcome.
type MessageDeliverySignature struct {
	Topic         string         `json:"topic"`
	PublishCount  int            `json:"publish_count"`
	ConsumeCounts map[string]int `json:"consume_counts"`
}

func MessageDelivery(events []model.Event, topic string) MessageDeliverySignature {
	sig := MessageDeliverySignature{Topic: topic, ConsumeCounts: map[string]int{}}
	for _, event := range events {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage || event.Attributes["topic"] != topic {
			continue
		}
		switch event.Attributes["message.action"] {
		case "publish":
			sig.PublishCount++
		case "consume":
			sig.ConsumeCounts[event.Service]++
		}
	}
	return sig
}

func EquivalentMessageDelivery(a, b MessageDeliverySignature) bool {
	if a.Topic != b.Topic || a.PublishCount != b.PublishCount || len(a.ConsumeCounts) != len(b.ConsumeCounts) {
		return false
	}
	for service, count := range a.ConsumeCounts {
		if b.ConsumeCounts[service] != count {
			return false
		}
	}
	return true
}
