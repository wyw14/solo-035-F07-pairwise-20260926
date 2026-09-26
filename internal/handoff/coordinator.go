package handoff

import (
	"context"
	"fmt"
	"sync"
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

// Coordinator orchestrates a controlled baton pass between two live sessions.
// The outgoing session keeps the program output until the incoming session has
// staged its own encoder, program and playout resources on preview and passes
// every readiness gate at the designated takeover instant.
type Coordinator struct {
	state      *State
	events     *event.State
	eventFlow  *event.Integration
	muxFlow    *mux.Coordinator
	playout    *playout.Coordinator
	encoders   *encoder.Coordinator
	captions   *caption.Coordinator
	continuity *caption.Continuity
	sync       *syncer.Coordinator
	quality    *quality.Coordinator
	clock      func() time.Time

	mu       sync.Mutex
	workerMu sync.Mutex
	cancels  map[model.CommandID]context.CancelFunc
}

func NewCoordinator(
	state *State,
	events *event.State,
	eventFlow *event.Integration,
	muxFlow *mux.Coordinator,
	playoutFlow *playout.Coordinator,
	encoders *encoder.Coordinator,
	captions *caption.Coordinator,
	continuity *caption.Continuity,
	syncFlow *syncer.Coordinator,
	qualityFlow *quality.Coordinator,
	clock func() time.Time,
) *Coordinator {
	if clock == nil {
		clock = time.Now
	}
	return &Coordinator{
		state:      state,
		events:     events,
		eventFlow:  eventFlow,
		muxFlow:    muxFlow,
		playout:    playoutFlow,
		encoders:   encoders,
		captions:   captions,
		continuity: continuity,
		sync:       syncFlow,
		quality:    qualityFlow,
		clock:      clock,
		cancels:    make(map[model.CommandID]context.CancelFunc),
	}
}

// Prepare arms a handover. The incoming session's program is placed on the
// director preview bus (overlapping the outgoing session's resources) and the
// takeover instant is recorded, but no output changes hands yet.
func (coordinator *Coordinator) Prepare(outgoingID, incomingID model.EventID, takeoverAt time.Time) (Handover, error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	now := coordinator.clock()
	outgoing, ok := coordinator.events.Get(outgoingID)
	if !ok {
		return Handover{}, fmt.Errorf("outgoing event %s not found", outgoingID)
	}
	incoming, ok := coordinator.events.Get(incomingID)
	if !ok {
		return Handover{}, fmt.Errorf("incoming event %s not found", incomingID)
	}
	if outgoing.Phase != model.PhaseOnAir && outgoing.Phase != model.PhaseFailedOver {
		return Handover{}, fmt.Errorf("outgoing event %s is %s, not holding an output", outgoingID, outgoing.Phase)
	}
	if incoming.Phase != model.PhaseArmed && incoming.Phase != model.PhaseRehearsal {
		return Handover{}, fmt.Errorf("incoming event %s is %s, not armed for takeover", incomingID, incoming.Phase)
	}
	program, ok := coordinator.muxStateProgram(incomingID)
	if !ok {
		return Handover{}, fmt.Errorf("incoming event %s has not configured a program", incomingID)
	}
	if program.Revision != incoming.Revision {
		return Handover{}, fmt.Errorf("incoming event %s advanced its program after configuration", incomingID)
	}
	output, ok := coordinator.playout.Output(incomingID)
	if !ok || output.OnAir || output.Revision != incoming.Revision {
		return Handover{}, fmt.Errorf("incoming event %s has no staged playout generation", incomingID)
	}
	if lease, ok := coordinator.encoders.LeaseByID(program.Encoder); !ok || lease.Owner != incomingID {
		return Handover{}, fmt.Errorf("incoming event %s does not hold its preview encoder lease", incomingID)
	}
	if takeoverAt.IsZero() {
		takeoverAt = now
	}
	handover, err := coordinator.state.Begin(outgoing, incoming, takeoverAt, now)
	if err != nil {
		return Handover{}, err
	}
	if err := coordinator.muxFlow.Preview(incomingID, incoming.Revision); err != nil {
		coordinator.state.Abort(handover.Token, err.Error(), now)
		return Handover{}, err
	}
	return handover, nil
}

func (coordinator *Coordinator) muxStateProgram(eventID model.EventID) (mux.Program, bool) {
	for _, program := range coordinator.muxFlow.Programs() {
		if program.EventID == eventID {
			return program, true
		}
	}
	return mux.Program{}, false
}

// Execute performs the controlled cut at the recorded takeover instant. When
// the incoming session is not ready the attempt is aborted and the outgoing
// session keeps the output untouched.
func (coordinator *Coordinator) Execute(token model.CommandID) (Handover, error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	now := coordinator.clock()
	handover, ok := coordinator.state.Get(token)
	if !ok {
		return Handover{}, fmt.Errorf("handover %s does not exist", token)
	}
	if !handover.Pending() {
		return handover, fmt.Errorf("handover %s is already %s", token, handover.Status)
	}
	if now.Before(handover.TakeoverAt) {
		return handover, fmt.Errorf("takeover instant %s has not arrived", handover.TakeoverAt)
	}
	if reason := coordinator.readinessReason(handover); reason != "" {
		aborted, _ := coordinator.state.Abort(token, reason, now)
		return aborted, fmt.Errorf("handover aborted: %s", reason)
	}

	incoming, _ := coordinator.events.Get(handover.Incoming)
	outgoing, _ := coordinator.events.Get(handover.Outgoing)
	output, _ := coordinator.playout.Output(incoming.ID)

	// The playout plane performs the only durable I/O step and rolls its output
	// back itself if the on-air proof cannot be persisted.
	if err := coordinator.playout.Cut(incoming, outgoing, output.Fence, map[string]any{
		"offset_ms":         0,
		"source_generation": incoming.SourceGeneration,
		"took_over_from":    outgoing.ID,
	}); err != nil {
		aborted, _ := coordinator.state.Abort(token, err.Error(), now)
		return aborted, fmt.Errorf("handover aborted: %w", err)
	}
	if err := coordinator.muxFlow.PromoteToAir(incoming.ID, outgoing.ID, incoming.Revision); err != nil {
		coordinator.playout.RevertCut(incoming.ID, outgoing.ID)
		aborted, _ := coordinator.state.Abort(token, err.Error(), now)
		return aborted, fmt.Errorf("handover aborted: %w", err)
	}
	if _, err := coordinator.eventFlow.MarkOnAir(incoming.ID); err != nil {
		coordinator.muxFlow.RevertPromotion(incoming.ID, outgoing.ID)
		coordinator.playout.RevertCut(incoming.ID, outgoing.ID)
		aborted, _ := coordinator.state.Abort(token, err.Error(), now)
		return aborted, fmt.Errorf("handover aborted: %w", err)
	}
	completed, err := coordinator.state.Complete(token, incoming.Revision, now)
	if err != nil {
		// The cut itself succeeded; surface the bookkeeping error without
		// reversing a cut the audience has already followed.
		return completed, err
	}
	coordinator.retireOutgoing(outgoing.ID)
	return completed, nil
}

// readinessReason re-checks every interlock against live state at the instant
// of the cut; an empty string means the incoming session may take over.
func (coordinator *Coordinator) readinessReason(handover Handover) string {
	outgoing, ok := coordinator.events.Get(handover.Outgoing)
	if !ok || (outgoing.Phase != model.PhaseOnAir && outgoing.Phase != model.PhaseFailedOver) {
		return "outgoing session no longer holds the output"
	}
	incoming, ok := coordinator.events.Get(handover.Incoming)
	if !ok {
		return "incoming session disappeared"
	}
	if incoming.Phase != model.PhaseArmed && incoming.Phase != model.PhaseRehearsal {
		return fmt.Sprintf("incoming session is %s", incoming.Phase)
	}
	if incoming.Revision != handover.IncomingRevision {
		return "incoming program revision advanced after preview"
	}
	program, ok := coordinator.muxStateProgram(incoming.ID)
	if !ok || program.Revision != incoming.Revision {
		return "incoming program is no longer staged"
	}
	if !coordinator.muxFlow.Previewing(incoming.ID) {
		return "incoming program lost its director preview"
	}
	if lease, ok := coordinator.encoders.LeaseByID(program.Encoder); !ok || lease.Owner != incoming.ID {
		return "incoming encoder lease is no longer held"
	}
	output, ok := coordinator.playout.Output(incoming.ID)
	if !ok || output.OnAir || output.Revision != incoming.Revision {
		return "incoming playout generation is not staged"
	}
	if !coordinator.sync.Ready(incoming.ID) {
		if status, synced := coordinator.sync.Status(incoming.ID); synced && status.Reason != "" {
			return "frame synchronization is not stable: " + status.Reason
		}
		return "frame synchronization is not stable"
	}
	if err := coordinator.captions.Ready(incoming.ID, incoming.Revision); err != nil {
		return err.Error()
	}
	if _, err := coordinator.quality.Permit(incoming.ID, true, true); err != nil {
		return err.Error()
	}
	return ""
}

// retireOutgoing closes the outgoing session by event ownership after a
// successful cut: output stops, the event holds, its caption chain freezes, and
// its encoder lease is released for the next session. The last frame-sync and
// caption evidence is retained under the event rather than reset, so the
// finished session reads as cleanly retired instead of failing an interlock.
func (coordinator *Coordinator) retireOutgoing(outgoingID model.EventID) {
	coordinator.muxFlow.Stop(outgoingID)
	coordinator.playout.Stop(outgoingID)
	coordinator.captions.Retire(outgoingID)
	coordinator.continuity.Retire(outgoingID)
	if lease, ok := coordinator.encoders.LeaseFor(outgoingID); ok {
		_ = coordinator.encoders.Release(lease)
	}
	// The output has already transferred; a phase bookkeeping problem must not
	// revoke the completed cut, so the hold is best-effort.
	_, _ = coordinator.eventFlow.Hold(outgoingID)
}

// Schedule fires a prepared cut after delay. The worker still goes through
// Execute, so an unready incoming session aborts at the instant and the
// outgoing session keeps the output.
func (coordinator *Coordinator) Schedule(parent context.Context, token model.CommandID, delay time.Duration) <-chan error {
	workerContext, cancel := context.WithCancel(parent)
	coordinator.workerMu.Lock()
	if existing := coordinator.cancels[token]; existing != nil {
		existing()
	}
	coordinator.cancels[token] = cancel
	coordinator.workerMu.Unlock()

	result := make(chan error, 1)
	go func() {
		defer close(result)
		defer func() {
			coordinator.workerMu.Lock()
			delete(coordinator.cancels, token)
			coordinator.workerMu.Unlock()
		}()
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-workerContext.Done():
			result <- workerContext.Err()
		case <-timer.C:
			_, err := coordinator.Execute(token)
			result <- err
		}
	}()
	return result
}

// CancelWithdraw abandons a pending handover before its cut and releases the
// incoming session's preview claim without touching the outgoing output.
func (coordinator *Coordinator) CancelWithdraw(token model.CommandID) (Handover, error) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	coordinator.workerMu.Lock()
	if cancel := coordinator.cancels[token]; cancel != nil {
		cancel()
		delete(coordinator.cancels, token)
	}
	coordinator.workerMu.Unlock()
	handover, ok := coordinator.state.Get(token)
	if !ok {
		return Handover{}, fmt.Errorf("handover %s does not exist", token)
	}
	updated, err := coordinator.state.Cancel(token, coordinator.clock())
	if err != nil {
		return handover, err
	}
	coordinator.muxFlow.ClearPreview(handover.Incoming)
	return updated, nil
}

func (coordinator *Coordinator) Get(token model.CommandID) (Handover, bool) {
	return coordinator.state.Get(token)
}

func (coordinator *Coordinator) PendingForIncoming(incomingID model.EventID) (Handover, bool) {
	return coordinator.state.PendingForIncoming(incomingID)
}

func (coordinator *Coordinator) List() []Handover {
	return coordinator.state.List()
}
