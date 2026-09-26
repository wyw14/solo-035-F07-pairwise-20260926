package handoff

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type Status string

const (
	// StatusPreviewing marks a handover whose incoming session is on the
	// director preview bus, waiting for its takeover instant and readiness gate.
	StatusPreviewing Status = "previewing"
	// StatusOnAir marks a completed controlled cut; the incoming session owns output.
	StatusOnAir Status = "on-air"
	// StatusAborted marks a takeover attempt that fired while the incoming
	// session was not ready; the outgoing session kept the output.
	StatusAborted Status = "aborted"
	// StatusCancelled marks a handover withdrawn before the cut.
	StatusCancelled Status = "cancelled"
)

// Handover tracks one baton pass from an outgoing session to an incoming one.
type Handover struct {
	Token            model.CommandID  `json:"token"`
	Outgoing         model.EventID    `json:"outgoing_event_id"`
	Incoming         model.EventID    `json:"incoming_event_id"`
	OutgoingRevision model.RevisionID `json:"outgoing_revision"`
	IncomingRevision model.RevisionID `json:"incoming_revision"`
	TakeoverAt       time.Time        `json:"takeover_at"`
	Status           Status           `json:"status"`
	Reason           string           `json:"reason,omitempty"`
	CreatedAt        time.Time        `json:"created_at"`
	UpdatedAt        time.Time        `json:"updated_at"`
	CompletedAt      *time.Time       `json:"completed_at,omitempty"`
}

func (handover Handover) Pending() bool {
	return handover.Status == StatusPreviewing
}

type State struct {
	mu        sync.RWMutex
	handovers map[model.CommandID]Handover
}

func NewState() *State {
	return &State{handovers: make(map[model.CommandID]Handover)}
}

// Begin registers a baton pass. Only one pending handover may target an
// outgoing session at a time; re-preparing the same incoming pair is idempotent.
func (state *State) Begin(outgoing, incoming model.LiveEvent, takeoverAt time.Time, now time.Time) (Handover, error) {
	if !outgoing.ID.Valid() || !incoming.ID.Valid() || outgoing.ID == incoming.ID {
		return Handover{}, fmt.Errorf("handover event identities are incomplete or identical")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, existing := range state.handovers {
		if !existing.Pending() {
			continue
		}
		if existing.Outgoing == outgoing.ID || existing.Incoming == incoming.ID {
			if existing.Outgoing == outgoing.ID && existing.Incoming == incoming.ID {
				existing.IncomingRevision = incoming.Revision
				existing.OutgoingRevision = outgoing.Revision
				existing.TakeoverAt = takeoverAt.UTC()
				existing.UpdatedAt = now.UTC()
				state.handovers[existing.Token] = existing
				return existing, nil
			}
			return Handover{}, fmt.Errorf("a pending handover already involves event %s or %s", outgoing.ID, incoming.ID)
		}
	}
	handover := Handover{
		Token:            model.NewCommandID(),
		Outgoing:         outgoing.ID,
		Incoming:         incoming.ID,
		OutgoingRevision: outgoing.Revision,
		IncomingRevision: incoming.Revision,
		TakeoverAt:       takeoverAt.UTC(),
		Status:           StatusPreviewing,
		CreatedAt:        now.UTC(),
		UpdatedAt:        now.UTC(),
	}
	state.handovers[handover.Token] = handover
	return handover, nil
}

func (state *State) update(token model.CommandID, mutate func(Handover) (Handover, error)) (Handover, error) {
	current, ok := state.handovers[token]
	if !ok {
		return Handover{}, fmt.Errorf("handover %s does not exist", token)
	}
	if !current.Pending() {
		return current, fmt.Errorf("handover %s is already %s", token, current.Status)
	}
	updated, err := mutate(current)
	if err != nil {
		return current, err
	}
	state.handovers[token] = updated
	return updated, nil
}

// Complete records the successful controlled cut.
func (state *State) Complete(token model.CommandID, revision model.RevisionID, now time.Time) (Handover, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.update(token, func(handover Handover) (Handover, error) {
		if revision != handover.IncomingRevision {
			return handover, fmt.Errorf("incoming program revision advanced after preview")
		}
		completed := now.UTC()
		handover.Status = StatusOnAir
		handover.CompletedAt = &completed
		handover.UpdatedAt = completed
		return handover, nil
	})
}

// Abort records a takeover that could not cut because the incoming session was
// not ready; the outgoing session remains on air and may be handed over again.
func (state *State) Abort(token model.CommandID, reason string, now time.Time) (Handover, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.update(token, func(handover Handover) (Handover, error) {
		handover.Status = StatusAborted
		handover.Reason = reason
		handover.UpdatedAt = now.UTC()
		return handover, nil
	})
}

// Cancel withdraws a handover before the cut and releases its preview claim.
func (state *State) Cancel(token model.CommandID, now time.Time) (Handover, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.update(token, func(handover Handover) (Handover, error) {
		handover.Status = StatusCancelled
		handover.UpdatedAt = now.UTC()
		return handover, nil
	})
}

func (state *State) Get(token model.CommandID) (Handover, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	handover, ok := state.handovers[token]
	return handover, ok
}

// PendingFor returns the pending handover targeting an outgoing session, if any.
func (state *State) PendingFor(outgoing model.EventID) (Handover, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	for _, handover := range state.handovers {
		if handover.Pending() && handover.Outgoing == outgoing {
			return handover, true
		}
	}
	return Handover{}, false
}

// PendingForIncoming returns the pending handover staged for an incoming
// session, if any.
func (state *State) PendingForIncoming(incoming model.EventID) (Handover, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	for _, handover := range state.handovers {
		if handover.Pending() && handover.Incoming == incoming {
			return handover, true
		}
	}
	return Handover{}, false
}

func (state *State) List() []Handover {
	state.mu.RLock()
	defer state.mu.RUnlock()
	items := make([]Handover, 0, len(state.handovers))
	for _, handover := range state.handovers {
		items = append(items, handover)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	return items
}
