package tracecontext

import "testing"

func TestRoundTrip(t *testing.T) {
	want := Context{TraceID: "00000000000000000000000000000001", SpanID: "0000000000000001", Flags: 1}
	got, err := Parse(want.String())
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("got=%+v want=%+v", got, want)
	}
}

func TestRejectsInvalidAndAllZeroIDs(t *testing.T) {
	cases := []string{
		"00-00000000000000000000000000000000-0000000000000001-01",
		"00-00000000000000000000000000000001-0000000000000000-01",
		"01-00000000000000000000000000000001-0000000000000001-01",
		"garbage",
	}
	for _, tc := range cases {
		if _, err := Parse(tc); err == nil {
			t.Fatalf("expected error for %q", tc)
		}
	}
}
