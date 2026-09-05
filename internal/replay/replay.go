package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

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
// topic: how many application publish events occurred and how many correlated
// consumes each service recorded. This lets replay distinguish a duplicated
// delivery from an otherwise identical successful HTTP outcome without letting
// unrelated same-topic traffic satisfy the evidence gate.
type MessageDeliverySignature struct {
	Topic         string         `json:"topic"`
	PublishCount  int            `json:"publish_count"`
	ConsumeCounts map[string]int `json:"consume_counts"`
}

func MessageDelivery(events []model.Event, topic string) MessageDeliverySignature {
	topic = strings.TrimSpace(topic)
	sig := MessageDeliverySignature{Topic: topic, ConsumeCounts: map[string]int{}}
	publishedMessageIDs := map[string]struct{}{}
	for _, event := range events {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage || strings.TrimSpace(event.Attributes["topic"]) != topic || strings.TrimSpace(event.Attributes["message.action"]) != "publish" {
			continue
		}
		sig.PublishCount++
		messageID := strings.TrimSpace(event.Attributes["message.id"])
		if messageID != "" {
			publishedMessageIDs[messageID] = struct{}{}
		}
	}
	for _, event := range events {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage || strings.TrimSpace(event.Attributes["topic"]) != topic || strings.TrimSpace(event.Attributes["message.action"]) != "consume" {
			continue
		}
		messageID := strings.TrimSpace(event.Attributes["message.id"])
		if _, ok := publishedMessageIDs[messageID]; !ok || messageID == "" {
			continue
		}
		sig.ConsumeCounts[strings.TrimSpace(event.Service)]++
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

// MessageDelaySignature captures the earliest observed publish-to-consume delay
// for a topic while deliberately excluding generated message identifiers from
// the signature. Correlation still uses message.id internally so unrelated
// messages cannot satisfy a delayed-delivery replay gate.
type MessageDelaySignature struct {
	Topic                  string `json:"topic"`
	PublishCount           int    `json:"publish_count"`
	CorrelatedConsumeCount int    `json:"correlated_consume_count"`
	MinConsumeDelayMicros  int64  `json:"min_consume_delay_us"`
}

type publishEvidence struct {
	timestamp time.Time
	count     int
}

func MessageDelay(events []model.Event, topic string) MessageDelaySignature {
	topic = strings.TrimSpace(topic)
	sig := MessageDelaySignature{Topic: topic}
	publishes := map[string]publishEvidence{}
	for _, event := range events {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage || strings.TrimSpace(event.Attributes["topic"]) != topic || strings.TrimSpace(event.Attributes["message.action"]) != "publish" {
			continue
		}
		sig.PublishCount++
		messageID := strings.TrimSpace(event.Attributes["message.id"])
		if messageID == "" {
			continue
		}
		current := publishes[messageID]
		current.count++
		if current.timestamp.IsZero() || event.Timestamp.Before(current.timestamp) {
			current.timestamp = event.Timestamp
		}
		publishes[messageID] = current
	}

	var haveDelay bool
	for _, event := range events {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage || strings.TrimSpace(event.Attributes["topic"]) != topic || strings.TrimSpace(event.Attributes["message.action"]) != "consume" {
			continue
		}
		published, ok := publishes[strings.TrimSpace(event.Attributes["message.id"])]
		if !ok || published.count != 1 {
			continue
		}
		if event.Timestamp.Before(published.timestamp) {
			continue
		}
		delay := event.Timestamp.Sub(published.timestamp).Microseconds()
		sig.CorrelatedConsumeCount++
		if !haveDelay || delay < sig.MinConsumeDelayMicros {
			sig.MinConsumeDelayMicros = delay
			haveDelay = true
		}
	}
	return sig
}

// MeetsMinimumMessageDelay is a threshold predicate, not an exact timing
// equality check. Replay can therefore prove that a scheduled delay reappeared
// without pretending process scheduling or wall-clock timing is deterministic.
func MeetsMinimumMessageDelay(sig MessageDelaySignature, minimum time.Duration) bool {
	return minimum > 0 && sig.PublishCount > 0 && sig.CorrelatedConsumeCount > 0 && sig.MinConsumeDelayMicros >= minimum.Microseconds()
}
