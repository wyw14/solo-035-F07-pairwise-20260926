package caption

import (
	"fmt"
	"sync"

	"github.com/wyw14/solo-035/internal/model"
)

type Cue struct {
	Revision model.RevisionID `json:"revision"`
	Sequence int64            `json:"sequence"`
	Text     string           `json:"text"`
}

type Continuity struct {
	mu      sync.Mutex
	last    map[model.EventID]Cue
	retired map[model.EventID]bool
}

func NewContinuity() *Continuity {
	return &Continuity{last: make(map[model.EventID]Cue), retired: make(map[model.EventID]bool)}
}

func (continuity *Continuity) Accept(eventID model.EventID, cue Cue) error {
	continuity.mu.Lock()
	defer continuity.mu.Unlock()
	if continuity.retired[eventID] {
		return fmt.Errorf("caption continuity for event %s is retired", eventID)
	}
	previous, ok := continuity.last[eventID]
	if ok && previous.Revision == cue.Revision && cue.Sequence != previous.Sequence+1 {
		return fmt.Errorf("caption cue sequence is discontinuous")
	}
	if ok && previous.Revision != cue.Revision && cue.Sequence != 1 {
		return fmt.Errorf("new caption revision must begin at sequence one")
	}
	if cue.Text == "" {
		return fmt.Errorf("caption cue is empty")
	}
	continuity.last[eventID] = cue
	return nil
}

// Retire freezes an outgoing event's continuity ledger so cues observed after
// the handover cannot be attributed to the finished session.
func (continuity *Continuity) Retire(eventID model.EventID) {
	continuity.mu.Lock()
	defer continuity.mu.Unlock()
	continuity.retired[eventID] = true
}

func (continuity *Continuity) Retired(eventID model.EventID) bool {
	continuity.mu.Lock()
	defer continuity.mu.Unlock()
	return continuity.retired[eventID]
}

func (continuity *Continuity) Last(eventID model.EventID) (Cue, bool) {
	continuity.mu.Lock()
	defer continuity.mu.Unlock()
	cue, ok := continuity.last[eventID]
	return cue, ok
}
