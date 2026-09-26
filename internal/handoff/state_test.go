package handoff

import (
	"testing"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

func testEvents() (model.LiveEvent, model.LiveEvent) {
	now := time.Unix(1000, 0).UTC()
	outgoing, _ := model.NewLiveEvent("evening news", now)
	outgoing.Phase = model.PhaseOnAir
	incoming, _ := model.NewLiveEvent("late show", now)
	incoming.Phase = model.PhaseArmed
	return outgoing, incoming
}

func TestBeginRegistersPendingHandover(t *testing.T) {
	state := NewState()
	outgoing, incoming := testEvents()
	takeover := time.Unix(2000, 0).UTC()

	handover, err := state.Begin(outgoing, incoming, takeover, time.Unix(1500, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if !handover.Pending() || handover.Status != StatusPreviewing {
		t.Fatalf("status = %s", handover.Status)
	}
	if handover.Outgoing != outgoing.ID || handover.Incoming != incoming.ID {
		t.Fatal("handover endpoints not recorded")
	}
	if !handover.TakeoverAt.Equal(takeover) {
		t.Fatalf("takeover = %s", handover.TakeoverAt)
	}
}

func TestBeginRejectsConflictingPendingPair(t *testing.T) {
	state := NewState()
	outgoing, incoming := testEvents()
	now := time.Unix(1500, 0).UTC()
	if _, err := state.Begin(outgoing, incoming, time.Unix(2000, 0).UTC(), now); err != nil {
		t.Fatal(err)
	}
	other, _ := model.NewLiveEvent("overnight", now)
	other.Phase = model.PhaseArmed
	if _, err := state.Begin(outgoing, other, time.Unix(2000, 0).UTC(), now); err == nil {
		t.Fatal("expected conflict on the same outgoing session")
	}
	if _, err := state.Begin(other, incoming, time.Unix(2000, 0).UTC(), now); err == nil {
		t.Fatal("expected conflict on the same incoming session")
	}
}

func TestBeginSamePairIsIdempotentAndUpdatesInstant(t *testing.T) {
	state := NewState()
	outgoing, incoming := testEvents()
	now := time.Unix(1500, 0).UTC()
	first, err := state.Begin(outgoing, incoming, time.Unix(2000, 0).UTC(), now)
	if err != nil {
		t.Fatal(err)
	}
	incoming = incoming.NewProgram(now.Add(time.Second))
	second, err := state.Begin(outgoing, incoming, time.Unix(2500, 0).UTC(), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if first.Token != second.Token {
		t.Fatalf("tokens differ: %s vs %s", first.Token, second.Token)
	}
	if second.IncomingRevision != incoming.Revision || !second.TakeoverAt.Equal(time.Unix(2500, 0).UTC()) {
		t.Fatal("re-prepared handover did not refresh its target")
	}
}

func TestCompleteAbortAndCancelAreTerminal(t *testing.T) {
	state := NewState()
	outgoing, incoming := testEvents()
	now := time.Unix(1500, 0).UTC()
	handover, err := state.Begin(outgoing, incoming, now, now)
	if err != nil {
		t.Fatal(err)
	}

	completed, err := state.Complete(handover.Token, incoming.Revision, now)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != StatusOnAir || completed.CompletedAt == nil {
		t.Fatalf("status = %s", completed.Status)
	}
	if _, err := state.Cancel(handover.Token, now); err == nil {
		t.Fatal("terminal handover must not be cancellable")
	}

	aborted := NewState()
	next, _ := aborted.Begin(outgoing, incoming, now, now)
	got, err := aborted.Abort(next.Token, "frame synchronization is not stable", now)
	if err != nil || got.Status != StatusAborted || got.Reason == "" {
		t.Fatalf("abort = %+v, %v", got, err)
	}
	if _, err := aborted.Complete(next.Token, incoming.Revision, now); err == nil {
		t.Fatal("aborted handover must not complete")
	}
}

func TestCompleteRejectsAdvancedRevision(t *testing.T) {
	state := NewState()
	outgoing, incoming := testEvents()
	now := time.Unix(1500, 0).UTC()
	handover, _ := state.Begin(outgoing, incoming, now, now)
	advanced := incoming.NewProgram(now)
	if _, err := state.Complete(handover.Token, advanced.Revision, now); err == nil {
		t.Fatal("cut must not complete against a newer program revision")
	}
}
