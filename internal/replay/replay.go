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
// topic: how many application publish events occurred and how many causally
// correlated consumes each service recorded. This lets replay distinguish a
// duplicated delivery from an otherwise identical successful HTTP outcome
// without letting unrelated or temporally impossible same-topic traffic satisfy
// the evidence gate.
type MessageDeliverySignature struct {
	Topic         string         `json:"topic"`
	PublishCount  int            `json:"publish_count"`
	ConsumeCounts map[string]int `json:"consume_counts"`
}

type messagePublishEvidence struct {
	timestamp time.Time
	sequence  uint64
}

type messageCorrelationKey struct {
	traceID   string
	messageID string
}

func correlationKey(event model.Event) messageCorrelationKey {
	return messageCorrelationKey{
		traceID:   strings.TrimSpace(event.TraceID),
		messageID: strings.TrimSpace(event.Attributes["message.id"]),
	}
}

func (key messageCorrelationKey) complete() bool {
	return key.traceID != "" && key.messageID != ""
}

func MessageDelivery(events []model.Event, topic string) MessageDeliverySignature {
	topic = strings.TrimSpace(topic)
	sig := MessageDeliverySignature{Topic: topic, ConsumeCounts: map[string]int{}}
	if topic == "" {
		return sig
	}
	publishes := map[messageCorrelationKey][]messagePublishEvidence{}
	for _, event := range events {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage || strings.TrimSpace(event.Attributes["topic"]) != topic || strings.TrimSpace(event.Attributes["message.action"]) != "publish" {
			continue
		}
		if strings.TrimSpace(event.Service) == "" {
			continue
		}
		sig.PublishCount++
		key := correlationKey(event)
		if !key.complete() {
			continue
		}
		publishes[key] = append(publishes[key], messagePublishEvidence{timestamp: event.Timestamp, sequence: event.Sequence})
	}
	for _, event := range events {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage || strings.TrimSpace(event.Attributes["topic"]) != topic || strings.TrimSpace(event.Attributes["message.action"]) != "consume" {
			continue
		}
		service := strings.TrimSpace(event.Service)
		if service == "" {
			continue
		}
		key := correlationKey(event)
		if !key.complete() {
			continue
		}
		if _, ok := uniquePrecedingPublish(publishes[key], event.Timestamp, event.Sequence); !ok {
			continue
		}
		sig.ConsumeCounts[service]++
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
// the signature. Correlation still uses trace_id and message.id internally so
// unrelated messages cannot satisfy a delayed-delivery replay gate.
type MessageDelaySignature struct {
	Topic                  string `json:"topic"`
	PublishCount           int    `json:"publish_count"`
	CorrelatedConsumeCount int    `json:"correlated_consume_count"`
	MinConsumeDelayMicros  int64  `json:"min_consume_delay_us"`
}

func MessageDelay(events []model.Event, topic string) MessageDelaySignature {
	topic = strings.TrimSpace(topic)
	sig := MessageDelaySignature{Topic: topic}
	if topic == "" {
		return sig
	}
	publishes := map[messageCorrelationKey][]messagePublishEvidence{}
	for _, event := range events {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage || strings.TrimSpace(event.Attributes["topic"]) != topic || strings.TrimSpace(event.Attributes["message.action"]) != "publish" {
			continue
		}
		if strings.TrimSpace(event.Service) == "" {
			continue
		}
		sig.PublishCount++
		key := correlationKey(event)
		if !key.complete() {
			continue
		}
		publishes[key] = append(publishes[key], messagePublishEvidence{timestamp: event.Timestamp, sequence: event.Sequence})
	}

	var haveDelay bool
	for _, event := range events {
		if event.Source != model.SourceApplication || event.Kind != model.KindMessage || strings.TrimSpace(event.Attributes["topic"]) != topic || strings.TrimSpace(event.Attributes["message.action"]) != "consume" {
			continue
		}
		if strings.TrimSpace(event.Service) == "" {
			continue
		}
		key := correlationKey(event)
		if !key.complete() {
			continue
		}
		published, ok := uniquePrecedingPublish(publishes[key], event.Timestamp, event.Sequence)
		if !ok {
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

func uniquePrecedingPublish(publishes []messagePublishEvidence, consumeTime time.Time, consumeSequence uint64) (messagePublishEvidence, bool) {
	var match messagePublishEvidence
	count := 0
	for _, publish := range publishes {
		if messageEvidencePrecedes(publish.timestamp, publish.sequence, consumeTime, consumeSequence) {
			match = publish
			count++
			if count > 1 {
				return messagePublishEvidence{}, false
			}
			continue
		}
		if !messageEvidenceFollows(publish.timestamp, publish.sequence, consumeTime, consumeSequence) {
			return messagePublishEvidence{}, false
		}
	}
	return match, count == 1
}

func messageEvidencePrecedes(publishTime time.Time, publishSequence uint64, consumeTime time.Time, consumeSequence uint64) bool {
	if publishTime.IsZero() || consumeTime.IsZero() {
		return false
	}
	if publishTime.Before(consumeTime) {
		return true
	}
	if !publishTime.Equal(consumeTime) {
		return false
	}
	if publishSequence == 0 || consumeSequence == 0 {
		return false
	}
	return publishSequence < consumeSequence
}

func messageEvidenceFollows(publishTime time.Time, publishSequence uint64, consumeTime time.Time, consumeSequence uint64) bool {
	if publishTime.IsZero() || consumeTime.IsZero() {
		return false
	}
	if publishTime.After(consumeTime) {
		return true
	}
	if !publishTime.Equal(consumeTime) {
		return false
	}
	if publishSequence == 0 || consumeSequence == 0 {
		return false
	}
	return publishSequence > consumeSequence
}

// MeetsMinimumMessageDelay is a threshold predicate, not an exact timing
// equality check. Replay can therefore prove that a scheduled delay reappeared
// without pretending process scheduling or wall-clock timing is deterministic.
func MeetsMinimumMessageDelay(sig MessageDelaySignature, minimum time.Duration) bool {
	return minimum > 0 && sig.PublishCount > 0 && sig.CorrelatedConsumeCount > 0 && sig.MinConsumeDelayMicros >= minimum.Microseconds()
}
