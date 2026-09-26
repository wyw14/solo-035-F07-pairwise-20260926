package handoff

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/caption"
	"github.com/wyw14/solo-035/internal/encoder"
	"github.com/wyw14/solo-035/internal/event"
	"github.com/wyw14/solo-035/internal/model"
	"github.com/wyw14/solo-035/internal/mux"
	"github.com/wyw14/solo-035/internal/playout"
	"github.com/wyw14/solo-035/internal/quality"
	"github.com/wyw14/solo-035/internal/syncer"
)

// Coordinator runs the controlled baton pass. It depends only on the per-event
// subsystems, never mutating one event's state on behalf of the other except
// through the explicit outgoing/incoming steps below.
type Coordinator struct {
	state      *State
	events     *event.Integration
	encoders   *encoder.Coordinator
	mux        *mux.Coordinator
	playout    *playout.Coordinator
	sync       *syncer.Coordinator
	captions   *caption.Coordinator
	continuity *caption.Continuity
	quality    *quality.Coordinator
	clock      func() time.Time
}

func NewCoordinator(
	state *State,
	events *event.Integration,
	encoders *encoder.Coordinator,
	muxFlow *mux.Coordinator,
	playoutFlow *playout.Coordinator,
	syncFlow *syncer.Coordinator,
	captionFlow *caption.Coordinator,
	continuity *caption.Continuity,
	qualityFlow *quality.Coordinator,
	clock func() time.Time,
) *Coordinator {
	return &Coordinator{
		state:      state,
		events:     events,
		encoders:   encoders,
		mux:        muxFlow,
		playout:    playoutFlow,
		sync:       syncFlow,
		captions:   captionFlow,
		continuity: continuity,
		quality:    qualityFlow,
		clock:      clock,
	}
}

// Preview overlaps director-preview resources: the incoming event binds a
// separate healthy encoder and gets a staged program, while the outgoing event
// keeps broadcasting on its own encoder. No output is published here.
func (coordinator *Coordinator) Preview(outgoing, incoming model.LiveEvent, source model.SourceID, batonAt time.Time) (Handoff, error) {
	if outgoing.Phase != model.PhaseOnAir && outgoing.Phase != model.PhaseFailedOver {
		return Handoff{}, fmt.Errorf("outgoing event %s is not on air (phase %s)", outgoing.ID, outgoing.Phase)
	}
	if onAir, ok := coordinator.playout.Output(outgoing.ID); !ok || !onAir.OnAir {
		return Handoff{}, fmt.Errorf("outgoing event %s is not publishing output", outgoing.ID)
	}
	if incoming.Phase != model.PhaseArmed {
		return Handoff{}, fmt.Errorf("incoming event %s cannot preview from phase %s", incoming.ID, incoming.Phase)
	}

	h, err := coordinator.state.Begin(outgoing.ID, incoming.ID, batonAt, coordinator.clock())
	if err != nil {
		return Handoff{}, err
	}

	lease, err := coordinator.encoders.ReservePreview(incoming.ID, "1080p50")
	if err != nil {
		// Contention must fail before the pass occupies a slot, so a retry can
		// bind resources once a spare encoder frees up.
		coordinator.state.Abort(h.ID)
		return Handoff{}, fmt.Errorf("incoming event cannot overlap preview resources: %w", err)
	}
	if err := coordinator.encoders.Start(lease); err != nil {
		coordinator.encoders.Release(lease)
		coordinator.state.Abort(h.ID)
		return Handoff{}, err
	}
	program, err := coordinator.mux.Configure(incoming, lease, 8000)
	if err != nil {
		coordinator.encoders.Release(lease)
		coordinator.state.Abort(h.ID)
		return Handoff{}, err
	}
	// Program is configured for preview but not activated into the mux; the
	// outgoing program remains the single active output until the cut.
	if err := coordinator.playout.Stage(incoming, source, lease.Fence, "handover-preview"); err != nil {
		coordinator.encoders.Release(lease)
		coordinator.state.Abort(h.ID)
		return Handoff{}, err
	}
	return coordinator.state.BindPreview(h.ID, lease, program.Revision, lease.Fence, coordinator.clock())
}

// MarkReady re-runs the incoming event's readiness gate ahead of the baton. A
// pass that fails stays previewing (or returns to previewing from deferred) so
// the director can fix evidence and arm again; the outgoing event is untouched.
func (coordinator *Coordinator) MarkReady(handoffID string) (Handoff, error) {
	h, ok := coordinator.state.Get(handoffID)
	if !ok {
		return Handoff{}, fmt.Errorf("handoff %s does not exist", handoffID)
	}
	incoming, ok := coordinator.eventRecord(h.Incoming)
	if !ok {
		return h, fmt.Errorf("incoming event %s not found", h.Incoming)
	}
	if err := coordinator.gate(incoming); err != nil {
		coordinator.state.NoteReadinessFailure(h.ID, err.Error(), coordinator.clock())
		return h, err
	}
	return coordinator.state.MarkReady(h.ID, coordinator.clock())
}

// gate is the same interlock set CommitOnAir enforces, applied to the incoming
// event while the outgoing event is still broadcasting.
func (coordinator *Coordinator) gate(event model.LiveEvent) error {
	if !coordinator.sync.Ready(event.ID) {
		return fmt.Errorf("frame synchronization is not stable")
	}
	if err := coordinator.captions.Ready(event.ID, event.Revision); err != nil {
		return err
	}
	if _, err := coordinator.quality.Permit(event.ID, true, true); err != nil {
		return err
	}
	return nil
}

// Execute performs the controlled cut at the baton moment. It re-checks
// readiness: when the incoming event is not ready the pass is deferred and the
// outgoing event keeps output. On success it is make-before-break — the
// incoming program is activated and published before the outgoing output,
// lease, and caption chain are settled.
func (coordinator *Coordinator) Execute(handoffID string) (Handoff, error) {
	h, ok := coordinator.state.Get(handoffID)
	if !ok {
		return Handoff{}, fmt.Errorf("handoff %s does not exist", handoffID)
	}
	if h.Phase == PhaseCompleted {
		return h, nil
	}
	if h.Phase == PhaseCancelled {
		return h, fmt.Errorf("handoff %s was cancelled", handoffID)
	}

	incoming, ok := coordinator.eventRecord(h.Incoming)
	if !ok {
		return coordinator.deferOrFail(h, "incoming event not found")
	}
	if h.Preview.Owner != incoming.ID {
		return coordinator.deferOrFail(h, "preview lease no longer belongs to incoming event")
	}

	// Re-run the gate at the baton moment; not ready means the outgoing event
	// holds output and the pass defers for a later attempt.
	if err := coordinator.gate(incoming); err != nil {
		return coordinator.deferOrFail(h, err.Error())
	}

	outgoing, ok := coordinator.eventRecord(h.Outgoing)
	if !ok {
		return coordinator.deferOrFail(h, "outgoing event not found")
	}
	currentOut, ok := coordinator.playout.Output(outgoing.ID)
	if !ok || !currentOut.OnAir {
		return coordinator.deferOrFail(h, "outgoing event stopped publishing before the cut")
	}

	// Make: bring the incoming event up first, with proof before publication.
	if err := coordinator.mux.Activate(incoming.ID, h.Revision); err != nil {
		return coordinator.deferOrFail(h, "incoming program activation failed: "+err.Error())
	}
	if err := coordinator.playout.CommitOnAir(incoming, h.Fence, map[string]any{
		"offset_ms":         0,
		"source_generation": incoming.SourceGeneration,
		"handover_from":     outgoing.ID,
	}); err != nil {
		return coordinator.deferOrFail(h, "incoming publication failed: "+err.Error())
	}
	if _, err := coordinator.events.MarkOnAir(incoming.ID); err != nil {
		return h, fmt.Errorf("incoming output is live but its phase did not advance: %w", err)
	}
	// Break: only now settle the outgoing event, attributed entirely to it.
	coordinator.mux.Stop(outgoing.ID)
	coordinator.playout.Stop(outgoing.ID)
	coordinator.continuity.Retire(outgoing.ID)
	coordinator.captions.Close(outgoing.ID)
	if released, err := coordinator.encoders.ReleaseFor(outgoing.ID); err != nil {
		return h, fmt.Errorf("incoming event is live but outgoing lease release failed: %w", err)
	} else if !released {
		return h, fmt.Errorf("incoming event is live but outgoing event held no lease to release")
	}
	if _, err := coordinator.events.Close(outgoing.ID); err != nil {
		return h, fmt.Errorf("incoming event is live but outgoing event did not close: %w", err)
	}

	return coordinator.state.Complete(h.ID, coordinator.clock())
}

// Retry re-arms a deferred pass against a new baton time and immediately
// re-runs the gate. The outgoing event has been holding output throughout.
func (coordinator *Coordinator) Retry(handoffID string, batonAt time.Time) (Handoff, error) {
	h, ok := coordinator.state.Get(handoffID)
	if !ok {
		return Handoff{}, fmt.Errorf("handoff %s does not exist", handoffID)
	}
	if h.Phase != PhaseDeferred {
		return h, fmt.Errorf("handoff %s cannot retry from phase %s", handoffID, h.Phase)
	}
	if _, err := coordinator.state.Reschedule(h.ID, batonAt, coordinator.clock()); err != nil {
		return h, err
	}
	incoming, ok := coordinator.eventRecord(h.Incoming)
	if !ok {
		return h, fmt.Errorf("incoming event %s not found", h.Incoming)
	}
	if err := coordinator.gate(incoming); err != nil {
		coordinator.state.NoteReadinessFailure(h.ID, err.Error(), coordinator.clock())
		return h, err
	}
	return coordinator.state.MarkReady(h.ID, coordinator.clock())
}

// Cancel abandons a pass and releases any preview resource the incoming event
// still held. Neither event changes on-air phase; the outgoing event keeps
// broadcasting.
func (coordinator *Coordinator) Cancel(handoffID string, reason string) (Handoff, error) {
	h, ok := coordinator.state.Get(handoffID)
	if !ok {
		return Handoff{}, fmt.Errorf("handoff %s does not exist", handoffID)
	}
	if h.Phase == PhaseCancelled || h.Phase == PhaseCompleted {
		return h, nil
	}
	if h.Preview.ID != "" {
		if lease, stillHeld := coordinator.encoders.LeaseFor(h.Incoming); stillHeld && lease.ID == h.Preview.ID {
			if err := coordinator.encoders.Release(lease); err != nil {
				return h, fmt.Errorf("release preview lease: %w", err)
			}
		}
	}
	coordinator.continuity.Retire(h.Incoming)
	return coordinator.state.Cancel(h.ID, reason, coordinator.clock())
}

func (coordinator *Coordinator) eventRecord(id model.EventID) (model.LiveEvent, bool) {
	return coordinator.events.Get(id)
}

// deferOrFail moves a not-yet-ready pass to deferred at the baton moment. A
// pass that is already deferred simply records another failed attempt. It
// returns the updated handoff so callers report the new phase, not their stale
// local copy.
func (coordinator *Coordinator) deferOrFail(h Handoff, reason string) (Handoff, error) {
	if h.Phase == PhaseDeferred {
		updated, _ := coordinator.state.NoteReadinessFailure(h.ID, reason, coordinator.clock())
		return updated, fmt.Errorf("handoff still deferred: %s", reason)
	}
	updated, _ := coordinator.state.Defer(h.ID, reason, coordinator.clock())
	return updated, fmt.Errorf("handoff deferred, outgoing event holds output: %s", reason)
}

func (coordinator *Coordinator) Get(id string) (Handoff, bool) { return coordinator.state.Get(id) }

func (coordinator *Coordinator) List() []Handoff { return coordinator.state.List() }
