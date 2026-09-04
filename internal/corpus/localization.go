package corpus

import "time"

// LocalizationLatencyThreshold is part of the v1 corpus evaluation contract.
// It is intentionally fixed so localization regressions are comparable across
// runs; changing it requires a corpus/report version change.
const LocalizationLatencyThreshold = 20 * time.Millisecond

// LocalizationTruth is seeded corpus truth used only for evaluation after a
// candidate ranking has been produced. The graph localizer never reads fault
// events or this truth while constructing candidates.
type LocalizationTruth struct {
	Service   string `json:"service"`
	Operation string `json:"operation"`
}

// ExpectedLocalization returns v1 localization truth only for fault classes
// the current application-span localizer can defensibly evaluate. Async
// duplicate delivery is intentionally excluded until message-topology
// divergence is ranked directly.
func ExpectedLocalization(c Case) (LocalizationTruth, bool) {
	switch c.ID {
	case "inventory-timeout", "inventory-connection-reset", "inventory-pre-request-crash":
		return LocalizationTruth{Service: "inventory", Operation: "check"}, true
	default:
		return LocalizationTruth{}, false
	}
}
