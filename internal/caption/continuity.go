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
	mu   sync.Mutex
	last map[model.EventID]Cue
}

func NewContinuity() *Continuity {
	return &Continuity{last: make(map[model.EventID]Cue)}
}

func (continuity *Continuity) Accept(eventID model.EventID, cue Cue) error {
	continuity.mu.Lock()
	defer continuity.mu.Unlock()
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

func (continuity *Continuity) Last(eventID model.EventID) (Cue, bool) {
	continuity.mu.Lock()
	defer continuity.mu.Unlock()
	cue, ok := continuity.last[eventID]
	return cue, ok
}

// Retire drops the cue cursor for an event whose program has handed output
// away, so a re-armed event starts a fresh revision at sequence one instead of
// being validated against the previous event's cue stream.
func (continuity *Continuity) Retire(eventID model.EventID) {
	continuity.mu.Lock()
	defer continuity.mu.Unlock()
	delete(continuity.last, eventID)
}
