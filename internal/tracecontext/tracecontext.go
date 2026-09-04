package tracecontext

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const Header = "traceparent"

type Context struct {
	TraceID string
	SpanID  string
	Flags   byte
}

func (c Context) Valid() bool {
	return validHexID(c.TraceID, 16) && validHexID(c.SpanID, 8)
}

func (c Context) String() string {
	return fmt.Sprintf("00-%s-%s-%02x", c.TraceID, c.SpanID, c.Flags)
}

func Parse(v string) (Context, error) {
	parts := strings.Split(strings.TrimSpace(v), "-")
	if len(parts) != 4 {
		return Context{}, errors.New("traceparent must contain 4 fields")
	}
	if parts[0] != "00" {
		return Context{}, fmt.Errorf("unsupported traceparent version %q", parts[0])
	}
	c := Context{TraceID: strings.ToLower(parts[1]), SpanID: strings.ToLower(parts[2])}
	if !validHexID(c.TraceID, 16) {
		return Context{}, errors.New("invalid trace id")
	}
	if !validHexID(c.SpanID, 8) {
		return Context{}, errors.New("invalid span id")
	}
	flags, err := hex.DecodeString(parts[3])
	if err != nil || len(flags) != 1 {
		return Context{}, errors.New("invalid trace flags")
	}
	c.Flags = flags[0]
	return c, nil
}

func NewTraceID() string {
	var b [16]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			panic(fmt.Sprintf("generate trace id: %v", err))
		}
		id := hex.EncodeToString(b[:])
		if validHexID(id, 16) {
			return id
		}
	}
}

func NewSpanID() string {
	var b [8]byte
	for {
		if _, err := rand.Read(b[:]); err != nil {
			panic(fmt.Sprintf("generate span id: %v", err))
		}
		id := hex.EncodeToString(b[:])
		if validHexID(id, 8) {
			return id
		}
	}
}

func validHexID(v string, bytes int) bool {
	if len(v) != bytes*2 {
		return false
	}
	raw, err := hex.DecodeString(v)
	if err != nil || len(raw) != bytes {
		return false
	}
	for _, b := range raw {
		if b != 0 {
			return true
		}
	}
	return false
}
