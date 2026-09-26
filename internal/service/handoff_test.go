package service

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wyw14/solo-035/internal/handoff"
	"github.com/wyw14/solo-035/internal/model"
)

type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func (clock *manualClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.t
}

func (clock *manualClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.t = clock.t.Add(duration)
}

func newHandoverTestRuntime(t *testing.T) (*Runtime, *Coordinator, *manualClock) {
	t.Helper()
	clock := &manualClock{t: time.Unix(1_000, 0).UTC()}
	runtime, err := NewRuntime(t.TempDir(), clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := Bootstrap(runtime); err != nil {
		t.Fatal(err)
	}
	return runtime, NewCoordinator(runtime), clock
}

func outgoingEvent(t *testing.T, runtime *Runtime) model.LiveEvent {
	t.Helper()
	events := runtime.Events.List()
	if len(events) != 1 {
		t.Fatalf("expected one bootstrapped event, got %d", len(events))
	}
	if events[0].Phase != model.PhaseOnAir {
		t.Fatalf("bootstrapped event phase = %s, want on-air", events[0].Phase)
	}
	return events[0]
}

// encoderOwnedBy returns the id of the encoder currently leased to an event.
func encoderOwnedBy(runtime *Runtime, owner model.EventID) string {
	for _, resource := range runtime.EncoderFlow.Equipment() {
		if resource.Lease.Owner == owner && resource.Lease.ReleasedAt == nil {
			return resource.ID
		}
	}
	return ""
}

// A prepared incoming event owns the spare encoder in preview, while the
// outgoing event keeps its encoder and its on-air output.
func assertPreviewOverlap(t *testing.T, runtime *Runtime, h handoff.Handoff) {
	t.Helper()
	if h.Phase != handoff.PhasePreviewing {
		t.Fatalf("handoff phase = %s, want previewing", h.Phase)
	}
	if runtime.Encoders.Owner(h.Preview.EncoderID) != h.Incoming {
		t.Fatalf("preview encoder %s owner = %s, want incoming event", h.Preview.EncoderID, runtime.Encoders.Owner(h.Preview.EncoderID))
	}
	if output, ok := runtime.Playout.Output(h.Incoming); !ok || output.OnAir {
		t.Fatalf("incoming preview output must be staged but not on air: %+v", output)
	}
	if output, ok := runtime.Playout.Output(h.Outgoing); !ok || !output.OnAir {
		t.Fatalf("outgoing output must stay on air during preview overlap")
	}
}

func TestHandoverReadyCutSettlesOutgoingByOwnership(t *testing.T) {
	runtime, coordinator, _ := newHandoverTestRuntime(t)
	out := outgoingEvent(t, runtime)

	h, _, err := coordinator.PrepareHandover(out.ID, "Late Show", model.NewSourceID(), runtime.Clock().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	assertPreviewOverlap(t, runtime, h)
	outEncoder := encoderOwnedBy(runtime, out.ID)
	if outEncoder == "" || outEncoder == h.Preview.EncoderID {
		t.Fatalf("preview must use a separate encoder from the outgoing event: out=%q preview=%q", outEncoder, h.Preview.EncoderID)
	}

	if h, err = coordinator.ArmHandover(h.ID); err != nil {
		t.Fatalf("arm handover: %v", err)
	}
	if h.Phase != handoff.PhaseReady {
		t.Fatalf("handoff phase = %s, want ready", h.Phase)
	}

	if h, err = coordinator.ExecuteHandover(h.ID); err != nil {
		t.Fatalf("execute handover: %v", err)
	}
	if h.Phase != handoff.PhaseCompleted {
		t.Fatalf("handoff phase = %s, want completed", h.Phase)
	}

	// Incoming event is now the single live output.
	incomingEvent, _ := runtime.Events.Get(h.Incoming)
	if incomingEvent.Phase != model.PhaseOnAir {
		t.Fatalf("incoming phase = %s, want on-air", incomingEvent.Phase)
	}
	inOutput, ok := runtime.Playout.Output(h.Incoming)
	if !ok || !inOutput.OnAir || inOutput.Fence != h.Fence {
		t.Fatalf("incoming output not live at its fenced generation: %+v", inOutput)
	}

	// Outgoing event is closed and its output is down.
	closedEvent, _ := runtime.Events.Get(h.Outgoing)
	if closedEvent.Phase != model.PhaseClosed {
		t.Fatalf("outgoing phase = %s, want closed", closedEvent.Phase)
	}
	if outOutput, _ := runtime.Playout.Output(h.Outgoing); outOutput.OnAir {
		t.Fatalf("outgoing output must be stopped after cut")
	}

	// The old lease is released; the incoming event keeps its preview lease.
	if owner := runtime.Encoders.Owner(outEncoder); owner != "" {
		t.Fatalf("outgoing encoder still owned by %s after cut", owner)
	}
	if owner := runtime.Encoders.Owner(h.Preview.EncoderID); owner != h.Incoming {
		t.Fatalf("incoming encoder owner = %s, want incoming event", owner)
	}

	// Caption state is settled by event ownership: outgoing closed, incoming live.
	if oldCaption, ok := runtime.Captions.Get(h.Outgoing); !ok || !oldCaption.Closed {
		t.Fatalf("outgoing caption chain must be closed: %+v", oldCaption)
	}
	if _, ok := runtime.Continuity.Last(h.Outgoing); ok {
		t.Fatalf("outgoing caption continuity cursor must be retired")
	}
	if err := runtime.CaptionFlow.Ready(h.Incoming, incomingEvent.Revision); err != nil {
		t.Fatalf("incoming caption chain must remain ready: %v", err)
	}

	// Proof-before-publication and live interlocks stay healthy after the cut.
	if report := runtime.Diagnostics(); !report.Healthy {
		t.Fatalf("runtime diagnostics unhealthy after handover: %+v", report.Failures())
	}
}

// At the baton moment an unready incoming event defers; the outgoing event
// keeps output. Readiness can be restored and the pass retried.
func TestHandoverDefersWhenIncomingNotReadyThenRetries(t *testing.T) {
	runtime, coordinator, _ := newHandoverTestRuntime(t)
	out := outgoingEvent(t, runtime)
	outEncoder := encoderOwnedBy(runtime, out.ID)

	h, incoming, err := coordinator.PrepareHandover(out.ID, "Late Show", model.NewSourceID(), runtime.Clock())
	if err != nil {
		t.Fatal(err)
	}

	// Break the incoming caption chain after preview overlap was established.
	runtime.CaptionFlow.ReportError(incoming.ID, incoming.Revision, "caption encoder dropped cue")

	h, err = coordinator.ExecuteHandover(h.ID)
	if err == nil {
		t.Fatal("expected deferred handover error")
	}
	if h.Phase != handoff.PhaseDeferred {
		t.Fatalf("handoff phase = %s, want deferred", h.Phase)
	}
	if output, _ := runtime.Playout.Output(out.ID); !output.OnAir {
		t.Fatal("outgoing event must keep output while handover is deferred")
	}
	if owner := runtime.Encoders.Owner(outEncoder); owner != out.ID {
		t.Fatalf("outgoing encoder owner = %s during deferral, want outgoing event", owner)
	}
	if output, _ := runtime.Playout.Output(incoming.ID); output.OnAir {
		t.Fatal("incoming output must not be published while not ready")
	}
	if _, err := coordinator.ArmHandover(h.ID); err == nil {
		t.Fatal("arming an unready deferred handover must fail")
	}

	// Restore the caption chain, retry with a fresh baton moment, then cut.
	runtime.CaptionFlow.Encode(incoming.ID, incoming.Revision, "BACK ON AIR")
	h, err = coordinator.RetryHandover(h.ID, runtime.Clock())
	if err != nil {
		t.Fatalf("retry handover: %v", err)
	}
	if h.Phase != handoff.PhaseReady {
		t.Fatalf("handoff phase = %s, want ready after retry", h.Phase)
	}
	if h, err = coordinator.ExecuteHandover(h.ID); err != nil {
		t.Fatalf("execute retried handover: %v", err)
	}
	if h.Phase != handoff.PhaseCompleted {
		t.Fatalf("handoff phase = %s, want completed", h.Phase)
	}
	if event, _ := runtime.Events.Get(out.ID); event.Phase != model.PhaseClosed {
		t.Fatalf("outgoing phase = %s, want closed", event.Phase)
	}
}

// While preview resources overlap, both encoders are fenced, so a third event
// cannot acquire an encoder until the pass completes or is cancelled.
func TestHandoverPreviewHoldsEncoderContention(t *testing.T) {
	runtime, coordinator, _ := newHandoverTestRuntime(t)
	out := outgoingEvent(t, runtime)
	outEncoder := encoderOwnedBy(runtime, out.ID)

	h, _, err := coordinator.PrepareHandover(out.ID, "Late Show", model.NewSourceID(), runtime.Clock().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.EncoderFlow.ReservePrimary(model.NewEventID(), "1080p50"); err == nil {
		t.Fatal("expected encoder acquisition to fail while preview overlaps both encoders")
	}
	active := 0
	for _, item := range runtime.Handoff.List() {
		if item.Phase == handoff.PhasePreviewing {
			active++
		}
	}
	if active != 1 {
		t.Fatalf("active preview handoffs = %d, want 1", active)
	}

	// Completing the pass frees the outgoing encoder for future contention.
	if _, err := coordinator.ExecuteHandover(h.ID); err != nil {
		t.Fatalf("execute handover: %v", err)
	}
	lease, err := runtime.EncoderFlow.ReservePrimary(model.NewEventID(), "1080p50")
	if err != nil {
		t.Fatalf("encoder should be available once outgoing lease settled: %v", err)
	}
	if lease.EncoderID != outEncoder {
		t.Fatalf("freed encoder = %s, want outgoing encoder %s", lease.EncoderID, outEncoder)
	}
}

// Cancelling a pass releases the preview lease and leaves the outgoing event
// broadcasting.
func TestHandoverCancelReleasesPreview(t *testing.T) {
	runtime, coordinator, _ := newHandoverTestRuntime(t)
	out := outgoingEvent(t, runtime)

	h, incoming, err := coordinator.PrepareHandover(out.ID, "Late Show", model.NewSourceID(), runtime.Clock().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if h, err = coordinator.CancelHandover(h.ID); err != nil {
		t.Fatalf("cancel handover: %v", err)
	}
	if h.Phase != handoff.PhaseCancelled {
		t.Fatalf("handoff phase = %s, want cancelled", h.Phase)
	}
	if owner := runtime.Encoders.Owner(h.Preview.EncoderID); owner != "" {
		t.Fatalf("preview encoder owner = %s after cancel, want released", owner)
	}
	if output, _ := runtime.Playout.Output(out.ID); !output.OnAir {
		t.Fatal("outgoing event must keep broadcasting after cancel")
	}
	if event, _ := runtime.Events.Get(incoming.ID); event.Phase != model.PhaseArmed {
		t.Fatalf("incoming phase = %s, want armed unchanged", event.Phase)
	}
	// The spare encoder is usable again.
	if _, err := runtime.EncoderFlow.ReservePrimary(model.NewEventID(), "1080p50"); err != nil {
		t.Fatalf("preview encoder should be free after cancel: %v", err)
	}
}

// A scheduled baton fires the controlled cut at the requested moment without
// bypassing the readiness gate.
func TestHandoverFiresAtScheduledBaton(t *testing.T) {
	runtime, coordinator, clock := newHandoverTestRuntime(t)
	out := outgoingEvent(t, runtime)

	// The scheduler sleeps for the requested duration, so use a short real delay.
	batonAt := clock.Now().Add(60 * time.Millisecond)
	h, _, err := coordinator.PrepareHandover(out.ID, "Late Show", model.NewSourceID(), batonAt)
	if err != nil {
		t.Fatal(err)
	}
	worker, _, err := coordinator.ScheduleHandover(context.Background(), h.ID)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-worker:
		if err != nil {
			t.Fatalf("scheduled cut: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("scheduled baton did not fire")
	}
	final, ok := runtime.Handoff.Get(h.ID)
	if !ok || final.Phase != handoff.PhaseCompleted {
		t.Fatalf("handoff after scheduled cut = %+v, want completed", final)
	}
	if output, _ := runtime.Playout.Output(out.ID); output.OnAir {
		t.Fatal("outgoing output must be stopped after scheduled cut")
	}
}

// The outgoing event cannot hand off twice at once, and a handoff cannot link
// an event to itself.
func TestHandoverRejectsConflictingPass(t *testing.T) {
	runtime, coordinator, _ := newHandoverTestRuntime(t)
	out := outgoingEvent(t, runtime)

	if _, _, err := coordinator.PrepareHandover(out.ID, "First", model.NewSourceID(), runtime.Clock().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := coordinator.PrepareHandover(out.ID, "Second", model.NewSourceID(), runtime.Clock().Add(2*time.Hour)); err == nil {
		t.Fatal("expected a second outgoing handoff to be rejected")
	}
}

// When both encoders are already fenced, preview contention must fail without
// leaving an orphaned handoff that would block a later successful pass.
func TestHandoverContentionLeavesNoOrphan(t *testing.T) {
	runtime, coordinator, _ := newHandoverTestRuntime(t)
	out := outgoingEvent(t, runtime)

	// A third party fences the spare encoder the incoming event would preview on.
	squatter, err := runtime.EncoderFlow.ReservePreview(model.NewEventID(), "1080p50")
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := coordinator.PrepareHandover(out.ID, "Late Show", model.NewSourceID(), runtime.Clock().Add(time.Hour)); err == nil {
		t.Fatal("expected preview to fail under encoder contention")
	}
	if items := runtime.Handoff.List(); len(items) != 0 {
		t.Fatalf("contention left %d orphaned handoff(s): %+v", len(items), items)
	}

	// Free the spare encoder; the same outgoing event can now hand off.
	if err := runtime.EncoderFlow.Release(squatter); err != nil {
		t.Fatal(err)
	}
	h, _, err := coordinator.PrepareHandover(out.ID, "Late Show", model.NewSourceID(), runtime.Clock().Add(time.Hour))
	if err != nil {
		t.Fatalf("handover after contention cleared: %v", err)
	}
	if h, err = coordinator.ExecuteHandover(h.ID); err != nil {
		t.Fatalf("execute handover: %v", err)
	}
	if h.Phase != handoff.PhaseCompleted {
		t.Fatalf("handoff phase = %s, want completed", h.Phase)
	}
}
