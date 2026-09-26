package handoff

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/wyw14/solo-035/internal/model"
)

// State stores active and finished handoffs. An event can appear at most once
// as an incoming event in a non-terminal handoff, and at most once as an
// outgoing event, so two passes cannot fight over the same output.
type State struct {
	mu       sync.RWMutex
	handoffs map[string]Handoff
}

func NewState() *State {
	return &State{handoffs: make(map[string]Handoff)}
}

// Begin records a new pass in PhasePreviewing.
func (state *State) Begin(outgoing, incoming model.EventID, batonAt time.Time, now time.Time) (Handoff, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, existing := range state.handoffs {
		if existing.terminal() {
			continue
		}
		if existing.Incoming == incoming {
			return Handoff{}, fmt.Errorf("event %s already has an active handoff", incoming)
		}
		if existing.Outgoing == outgoing {
			return Handoff{}, fmt.Errorf("event %s is already handing off to another event", outgoing)
		}
	}
	handoff := Handoff{
		ID:        uuid.NewString(),
		Outgoing:  outgoing,
		Incoming:  incoming,
		Phase:     PhasePreviewing,
		BatonAt:   batonAt.UTC(),
		CreatedAt: now.UTC(),
		UpdatedAt: now.UTC(),
	}
	if err := handoff.validate(); err != nil {
		return Handoff{}, err
	}
	state.handoffs[handoff.ID] = handoff
	return handoff, nil
}

// Get returns a handoff by id.
func (state *State) Get(id string) (Handoff, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	handoff, ok := state.handoffs[id]
	return handoff, ok
}

// ActiveForIncoming returns the non-terminal handoff that an event is the
// incoming side of, if any.
func (state *State) ActiveForIncoming(eventID model.EventID) (Handoff, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	for _, handoff := range state.handoffs {
		if !handoff.terminal() && handoff.Incoming == eventID {
			return handoff, true
		}
	}
	return Handoff{}, false
}

// mutate applies a guarded change to one handoff while holding the write lock.
func (state *State) mutate(id string, now time.Time, change func(Handoff) (Handoff, error)) (Handoff, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	current, ok := state.handoffs[id]
	if !ok {
		return Handoff{}, fmt.Errorf("handoff %s does not exist", id)
	}
	updated, err := change(current)
	if err != nil {
		return current, err
	}
	if updated.UpdatedAt.IsZero() {
		updated.UpdatedAt = now.UTC()
	}
	state.handoffs[id] = updated
	return updated, nil
}

// BindPreview attaches the preview lease and the staged program generation to
// the pass.
func (state *State) BindPreview(id string, lease model.EncoderLease, revision model.RevisionID, fence uint64, now time.Time) (Handoff, error) {
	return state.mutate(id, now, func(h Handoff) (Handoff, error) {
		if h.Phase != PhasePreviewing && h.Phase != PhaseDeferred {
			return h, fmt.Errorf("handoff %s cannot bind a preview lease in phase %s", id, h.Phase)
		}
		if lease.Owner != h.Incoming {
			return h, fmt.Errorf("preview lease belongs to another event")
		}
		h.Preview = lease
		h.Revision = revision
		h.Fence = fence
		return h, nil
	})
}

// MarkReady moves a previewing or deferred pass to ready.
func (state *State) MarkReady(id string, now time.Time) (Handoff, error) {
	return state.mutate(id, now, func(h Handoff) (Handoff, error) {
		return h.transition(PhaseReady, now)
	})
}

// Reschedule updates the baton time of a deferred pass so it can be retried.
func (state *State) Reschedule(id string, batonAt time.Time, now time.Time) (Handoff, error) {
	return state.mutate(id, now, func(h Handoff) (Handoff, error) {
		if h.Phase != PhaseDeferred {
			return h, fmt.Errorf("handoff %s cannot reschedule from phase %s", id, h.Phase)
		}
		h.BatonAt = batonAt.UTC()
		h.Reason = ""
		return h, nil
	})
}

// NoteReadinessFailure records why the gate failed and keeps the pass in its
// current phase (previewing before the baton, deferred at/after it).
func (state *State) NoteReadinessFailure(id string, reason string, now time.Time) (Handoff, error) {
	return state.mutate(id, now, func(h Handoff) (Handoff, error) {
		if h.terminal() {
			return h, fmt.Errorf("handoff %s is already %s", id, h.Phase)
		}
		h.Reason = reason
		h.Attempts++
		return h, nil
	})
}

// Defer moves the pass to deferred because the baton moment arrived before the
// incoming event was ready. The outgoing event keeps output.
func (state *State) Defer(id string, reason string, now time.Time) (Handoff, error) {
	return state.mutate(id, now, func(h Handoff) (Handoff, error) {
		h.Reason = reason
		h.Attempts++
		return h.transition(PhaseDeferred, now)
	})
}

// Complete finalizes a successful controlled cut.
func (state *State) Complete(id string, now time.Time) (Handoff, error) {
	return state.mutate(id, now, func(h Handoff) (Handoff, error) {
		updated, err := h.transition(PhaseCompleted, now)
		if err != nil {
			return h, err
		}
		completed := now.UTC()
		updated.CompletedAt = &completed
		return updated, nil
	})
}

// Cancel abandons the pass and marks why.
func (state *State) Cancel(id string, reason string, now time.Time) (Handoff, error) {
	return state.mutate(id, now, func(h Handoff) (Handoff, error) {
		h.Reason = reason
		return h.transition(PhaseCancelled, now)
	})
}

// Abort removes a pass that could never bind its preview resources. It is used
// during setup so encoder contention fails before a pass occupies a slot,
// rather than leaving an orphaned previewing record that blocks a retry.
func (state *State) Abort(id string) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	handoff, ok := state.handoffs[id]
	if !ok || handoff.terminal() {
		return false
	}
	delete(state.handoffs, id)
	return true
}

// List returns every handoff, newest first.
func (state *State) List() []Handoff {
	state.mu.RLock()
	defer state.mu.RUnlock()
	items := make([]Handoff, 0, len(state.handoffs))
	for _, handoff := range state.handoffs {
		items = append(items, handoff)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	return items
}
