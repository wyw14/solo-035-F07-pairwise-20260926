package service

import (
	"context"
	"testing"
	"time"

	"github.com/wyw14/solo-035/internal/caption"
	"github.com/wyw14/solo-035/internal/model"
)

func bootstrappedRuntime(t *testing.T) (*Runtime, *Coordinator) {
	t.Helper()
	runtime, err := NewRuntime(t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := Bootstrap(runtime); err != nil {
		t.Fatal(err)
	}
	return runtime, NewCoordinator(runtime)
}

func firstEventID(t *testing.T, runtime *Runtime) model.EventID {
	t.Helper()
	operations := runtime.Operations()
	if len(operations) != 1 {
		t.Fatalf("operations = %d, want 1", len(operations))
	}
	return operations[0].EventID
}

// rehearseIncoming arms and stages a new session on preview while another
// session stays on air.
func rehearseIncoming(t *testing.T, coordinator *Coordinator, name string) model.LiveEvent {
	t.Helper()
	incoming, err := coordinator.CreateEvent(name)
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.RehearseIncoming(incoming.ID, model.NewSourceID()); err != nil {
		t.Fatal(err)
	}
	return runtimeEvent(coordinator, incoming.ID)
}

func runtimeEvent(coordinator *Coordinator, id model.EventID) model.LiveEvent {
	return coordinator.runtime.Events.GetOrZero(id)
}

func TestHandoverOverlapsPreviewThenCutsControlled(t *testing.T) {
	runtime, coordinator := bootstrappedRuntime(t)
	outgoingID := firstEventID(t, runtime)

	incoming := rehearseIncoming(t, coordinator, "Late Show")

	// Before the handover the old session owns the output and encoder, while the
	// new session is fully staged but holds neither.
	if onAir := runtime.Mux.OnAirEvent(); onAir != outgoingID {
		t.Fatalf("on-air event = %s, want %s", onAir, outgoingID)
	}
	if runtime.Mux.Previewing(incoming.ID) {
		t.Fatal("incoming program must not be on preview before prepare")
	}

	hand, err := coordinator.PrepareHandover(outgoingID, incoming.ID, 0)
	if err != nil {
		t.Fatal(err)
	}

	// Overlap window: both sessions hold fenced encoders at once, the incoming
	// program is on preview, and the outgoing program still drives the output.
	owners := map[model.EventID]string{}
	for _, resource := range runtime.EncoderFlow.Equipment() {
		if resource.Lease.Owner != "" {
			owners[resource.Lease.Owner] = resource.ID
		}
	}
	if len(owners) != 2 {
		t.Fatalf("overlapping encoder owners = %v, want both sessions", owners)
	}
	if !runtime.Mux.Previewing(incoming.ID) {
		t.Fatal("incoming program must be on preview after prepare")
	}
	if runtime.Mux.OnAirEvent() != outgoingID {
		t.Fatal("outgoing session must keep the output during preview")
	}
	if output, _ := runtime.Playout.Output(outgoingID); !output.OnAir {
		t.Fatal("outgoing playout must remain on air during preview")
	}

	completed, err := coordinator.ExecuteHandover(hand.Token)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != "on-air" {
		t.Fatalf("handover status = %s", completed.Status)
	}

	// After the controlled cut the new session owns the output and phase.
	if runtime.Mux.OnAirEvent() != incoming.ID {
		t.Fatalf("on-air event = %s, want %s", runtime.Mux.OnAirEvent(), incoming.ID)
	}
	if output, _ := runtime.Playout.Output(incoming.ID); !output.OnAir {
		t.Fatal("incoming playout must be on air after the cut")
	}
	if output, _ := runtime.Playout.Output(outgoingID); output.OnAir {
		t.Fatal("outgoing playout must be retired after the cut")
	}
	if got := runtimeEvent(coordinator, incoming.ID).Phase; got != model.PhaseOnAir {
		t.Fatalf("incoming phase = %s", got)
	}
	if got := runtimeEvent(coordinator, outgoingID).Phase; got != model.PhaseHolding {
		t.Fatalf("outgoing phase = %s, want holding", got)
	}
	if runtime.Mux.Previewing(incoming.ID) {
		t.Fatal("incoming program must leave preview after the cut")
	}

	// Old lease and caption state are closed by event ownership.
	if lease, ok := runtime.EncoderFlow.LeaseFor(outgoingID); ok {
		t.Fatalf("outgoing encoder lease was not released: %+v", lease)
	}
	if !runtime.CaptionFlow.Retired(outgoingID) {
		t.Fatal("outgoing caption status must be retired")
	}
	if !runtime.Continuity.Retired(outgoingID) {
		t.Fatal("outgoing caption continuity must be retired")
	}
	if err := runtime.Continuity.Accept(outgoingID, caption.Cue{Revision: incoming.Revision, Sequence: 1, Text: "late cue"}); err == nil {
		t.Fatal("retired continuity ledger must reject later cues")
	}
	if incomingStatus, ok := runtime.Captions.Get(incoming.ID); !ok || !incomingStatus.Continuous {
		t.Fatal("incoming caption status must remain continuous")
	}

	// A clean handover leaves the whole control plane healthy: no interlock is
	// left failing for the retired session and exactly one output is on air.
	snapshot := runtime.Snapshot()
	if !snapshot.Healthy() {
		t.Fatal("snapshot is unhealthy after a controlled handover")
	}
	onAirCount := 0
	for _, operation := range snapshot.Operations {
		if operation.OnAir {
			onAirCount++
		}
		if operation.EventID == outgoingID && (operation.OnAir || operation.Preview) {
			t.Fatal("retired operation must not hold output or preview")
		}
	}
	if onAirCount != 1 {
		t.Fatalf("on-air operations = %d, want 1", onAirCount)
	}
	if !runtime.Diagnostics().Healthy {
		t.Fatal("diagnostics report a failure after a controlled handover")
	}
}

func TestHandoverAbortsWhenIncomingNotReadyAndKeepsOldOutput(t *testing.T) {
	runtime, coordinator := bootstrappedRuntime(t)
	outgoingID := firstEventID(t, runtime)
	incoming := rehearseIncoming(t, coordinator, "Late Show")

	hand, err := coordinator.PrepareHandover(outgoingID, incoming.ID, 0)
	if err != nil {
		t.Fatal(err)
	}

	// The incoming frame lock collapses between preview and the takeover instant.
	runtime.Sync.Close(incoming.ID)

	aborted, err := coordinator.ExecuteHandover(hand.Token)
	if err == nil {
		t.Fatal("expected the handover to abort")
	}
	if aborted.Status != "aborted" || aborted.Reason == "" {
		t.Fatalf("aborted handover = %+v", aborted)
	}

	if runtime.Mux.OnAirEvent() != outgoingID {
		t.Fatal("outgoing session must keep the output after an aborted cut")
	}
	if output, _ := runtime.Playout.Output(outgoingID); !output.OnAir {
		t.Fatal("outgoing playout must stay on air after an aborted cut")
	}
	if got := runtimeEvent(coordinator, outgoingID).Phase; got != model.PhaseOnAir {
		t.Fatalf("outgoing phase = %s", got)
	}
	if output, _ := runtime.Playout.Output(incoming.ID); output.OnAir {
		t.Fatal("incoming playout must not be on air after an aborted cut")
	}
	if _, ok := runtime.EncoderFlow.LeaseFor(outgoingID); !ok {
		t.Fatal("outgoing encoder lease must survive an aborted cut")
	}
	if runtime.CaptionFlow.Retired(outgoingID) {
		t.Fatal("outgoing caption state must not be retired after an aborted cut")
	}
	if !runtime.Mux.Previewing(incoming.ID) {
		t.Fatal("aborted handover must keep the incoming program on preview for a later attempt")
	}
	if !runtime.Diagnostics().Healthy {
		t.Fatal("diagnostics must stay healthy while the old session holds air after an abort")
	}

	// Once the incoming session is stable again the director re-arms the
	// handover and the second cut succeeds. Only the frame-sync window that
	// collapsed is rebuilt; audio evidence and the caption chain stay valid.
	syncStatus, _ := runtime.SyncState.Get(incoming.ID)
	if err := runtime.Sync.Open(incoming.ID, 3); err != nil {
		t.Fatal(err)
	}
	base := runtime.Clock().UTC()
	for sequence := int64(1); sequence <= 3; sequence++ {
		frame := model.Frame{
			Source:           syncStatus.Source,
			SourceGeneration: syncStatus.SourceGeneration,
			Sequence:         sequence,
			Timecode:         3000 + sequence,
			CapturedAt:       base.Add(time.Duration(sequence) * 20 * time.Millisecond),
			VideoLocked:      true,
			AudioLocked:      true,
		}
		if err := coordinator.PushFrame(incoming.ID, frame); err != nil {
			t.Fatal(err)
		}
	}
	retried, err := coordinator.PrepareHandover(outgoingID, incoming.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Token == hand.Token {
		t.Fatal("retried handover must be a new baton-pass record")
	}
	if _, err := coordinator.ExecuteHandover(retried.Token); err != nil {
		t.Fatalf("retried handover failed: %v", err)
	}
	if runtime.Mux.OnAirEvent() != incoming.ID {
		t.Fatal("retried handover must eventually move the output to the incoming session")
	}
	if got := runtimeEvent(coordinator, outgoingID).Phase; got != model.PhaseHolding {
		t.Fatalf("outgoing phase = %s, want holding", got)
	}

	// A terminal handover cannot be executed twice.
	if _, err := coordinator.ExecuteHandover(hand.Token); err == nil {
		t.Fatal("aborted handover must not execute again")
	}
}

func TestHandoverRespectsTakeoverInstantAndScheduledCut(t *testing.T) {
	runtime, coordinator := bootstrappedRuntime(t)
	outgoingID := firstEventID(t, runtime)
	incoming := rehearseIncoming(t, coordinator, "Late Show")

	hand, err := coordinator.PrepareHandover(outgoingID, incoming.ID, 120*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.ExecuteHandover(hand.Token); err == nil {
		t.Fatal("cut before the takeover instant must be refused")
	}
	if runtime.Mux.OnAirEvent() != outgoingID {
		t.Fatal("early execute attempt must not change the output")
	}

	worker, err := coordinator.ScheduleHandover(context.Background(), hand.Token)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-worker; err != nil {
		t.Fatal(err)
	}
	if runtime.Mux.OnAirEvent() != incoming.ID {
		t.Fatalf("scheduled cut did not hand output to %s", incoming.ID)
	}
	if got := runtimeEvent(coordinator, incoming.ID).Phase; got != model.PhaseOnAir {
		t.Fatalf("incoming phase = %s", got)
	}
}

func TestCancelHandoverReleasesPreviewOnly(t *testing.T) {
	runtime, coordinator := bootstrappedRuntime(t)
	outgoingID := firstEventID(t, runtime)
	incoming := rehearseIncoming(t, coordinator, "Late Show")

	hand, err := coordinator.PrepareHandover(outgoingID, incoming.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := coordinator.CancelHandover(hand.Token)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != "cancelled" {
		t.Fatalf("status = %s", cancelled.Status)
	}
	if runtime.Mux.Previewing(incoming.ID) {
		t.Fatal("cancelled handover must release the incoming preview")
	}
	if runtime.Mux.OnAirEvent() != outgoingID {
		t.Fatal("cancelled handover must not change the output")
	}
	if got := runtimeEvent(coordinator, outgoingID).Phase; got != model.PhaseOnAir {
		t.Fatalf("outgoing phase = %s", got)
	}
	if _, err := coordinator.CancelHandover(hand.Token); err == nil {
		t.Fatal("cancelled handover must not be cancellable again")
	}
}

func TestPrepareHandoverRejectsSessionWithoutStagedProgram(t *testing.T) {
	runtime, coordinator := bootstrappedRuntime(t)
	outgoingID := firstEventID(t, runtime)
	incoming, err := coordinator.CreateEvent("Unprepared")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.PrepareHandover(outgoingID, incoming.ID, 0); err == nil {
		t.Fatal("handover without a staged program must be rejected")
	}
}
