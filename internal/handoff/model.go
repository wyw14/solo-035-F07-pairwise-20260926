package handoff

import (
	"errors"
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

// Phase is the lifecycle of a single baton pass from an outgoing event to an
// incoming event.
type Phase string

const (
	// PhasePreviewing means the incoming event holds a preview encoder and a
	// staged program, while the outgoing event still owns the on-air output.
	PhasePreviewing Phase = "previewing"
	// PhaseReady means the incoming event passed every interlock and is armed
	// for the controlled cut at its handover time.
	PhaseReady Phase = "ready"
	// PhaseCompleted means the incoming event is on air and the outgoing event
	// has been settled and closed.
	PhaseCompleted Phase = "completed"
	// PhaseDeferred means the baton time arrived before the incoming event was
	// ready; the outgoing event keeps output and the pass can be retried.
	PhaseDeferred Phase = "deferred"
	// PhaseCancelled means the pass was abandoned; any preview lease was
	// released and neither event changed phase.
	PhaseCancelled Phase = "cancelled"
)

var phaseTransitions = map[Phase]map[Phase]bool{
	PhasePreviewing: {PhaseReady: true, PhaseDeferred: true, PhaseCancelled: true, PhaseCompleted: true},
	PhaseReady:      {PhaseCompleted: true, PhaseDeferred: true, PhaseCancelled: true, PhasePreviewing: true},
	PhaseDeferred:   {PhaseReady: true, PhaseCancelled: true, PhaseCompleted: true},
	PhaseCompleted:  {},
	PhaseCancelled:  {},
}

// Handoff records one directed baton pass. The two events intentionally keep
// their own independent program state; this record only coordinates the
// overlap, the scheduled cut, and the outgoing cleanup.
type Handoff struct {
	ID          string             `json:"id"`
	Outgoing    model.EventID      `json:"outgoing_event_id"`
	Incoming    model.EventID      `json:"incoming_event_id"`
	Phase       Phase              `json:"phase"`
	Preview     model.EncoderLease `json:"preview_lease"`
	Revision    model.RevisionID   `json:"revision"`
	Fence       uint64             `json:"fence"`
	BatonAt     time.Time          `json:"baton_at"`
	Reason      string             `json:"reason,omitempty"`
	Attempts    int                `json:"attempts"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`
	CompletedAt *time.Time         `json:"completed_at,omitempty"`
}

// terminal reports whether no further transition is possible.
func (h Handoff) terminal() bool {
	return h.Phase == PhaseCompleted || h.Phase == PhaseCancelled
}

func (h Handoff) canTransition(next Phase) bool {
	return phaseTransitions[h.Phase][next]
}

func (h Handoff) transition(next Phase, now time.Time) (Handoff, error) {
	if !h.canTransition(next) {
		return h, fmt.Errorf("handoff %s cannot move from %s to %s", h.ID, h.Phase, next)
	}
	h.Phase = next
	h.UpdatedAt = now.UTC()
	return h, nil
}

// validate makes sure a handoff links two distinct, well-formed events.
func (h Handoff) validate() error {
	if !h.Outgoing.Valid() {
		return errors.New("handoff outgoing event is invalid")
	}
	if !h.Incoming.Valid() {
		return errors.New("handoff incoming event is invalid")
	}
	if h.Outgoing == h.Incoming {
		return errors.New("handoff incoming and outgoing event must differ")
	}
	if h.BatonAt.IsZero() {
		return errors.New("handoff baton time is required")
	}
	return nil
}
