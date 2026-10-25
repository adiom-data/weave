package position

import (
	"github.com/adiom-data/weave/internal/querysubscription/model"
	"testing"
)

func TestTrackerEnforcesSourceOrderingAndPermitsRepeatedReports(t *testing.T) {
	var tracker Tracker
	for _, p := range []*querysubscription.Position{{SourceID: "database", Value: 10}, {SourceID: "database", Value: 11}, {SourceID: "other", Value: 1}} {
		if err := tracker.Observe(p); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range []*querysubscription.Position{{SourceID: "database", Value: 11}, {SourceID: "database", Value: 9}, {Value: 12}, nil} {
		if err := tracker.Observe(p); err == nil {
			t.Fatalf("accepted non-advancing position %+v", p)
		}
	}
	if err := tracker.ObserveReport(&querysubscription.Position{SourceID: "database", Value: 11}); err != nil {
		t.Fatal(err)
	}
	if err := tracker.ObserveReport(&querysubscription.Position{SourceID: "database", Value: 10}); err == nil {
		t.Fatal("accepted regressing report")
	}
}
