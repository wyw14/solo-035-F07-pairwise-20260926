package caption

import (
	"sync"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type State struct {
	mu       sync.RWMutex
	statuses map[model.EventID]model.CaptionStatus
}

func NewState() *State {
	return &State{statuses: make(map[model.EventID]model.CaptionStatus)}
}

func (state *State) Update(status model.CaptionStatus) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.statuses[status.EventID] = status
}

func (state *State) Success(eventID model.EventID, revision model.RevisionID, now time.Time) model.CaptionStatus {
	status := model.CaptionStatus{EventID: eventID, Revision: revision, Continuous: true, UpdatedAt: now.UTC()}
	state.Update(status)
	return status
}

func (state *State) Failure(eventID model.EventID, revision model.RevisionID, message string, now time.Time) model.CaptionStatus {
	status := model.CaptionStatus{EventID: eventID, Revision: revision, Continuous: false, EncoderErr: message, UpdatedAt: now.UTC()}
	state.Update(status)
	return status
}

// Close retires the caption chain that belongs to an event once that event has
// handed output away. The status is retained under the event id so the close
// remains attributable to the event that owned the cues, rather than deleting
// the evidence or reassigning it to the incoming event.
func (state *State) Close(eventID model.EventID, now time.Time) (model.CaptionStatus, bool) {
	state.mu.Lock()
	defer state.mu.Unlock()
	status, ok := state.statuses[eventID]
	if !ok {
		return model.CaptionStatus{}, false
	}
	status.Continuous = false
	status.Closed = true
	status.EncoderErr = ""
	status.UpdatedAt = now.UTC()
	state.statuses[eventID] = status
	return status, true
}

func (state *State) Get(eventID model.EventID) (model.CaptionStatus, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	status, ok := state.statuses[eventID]
	return status, ok
}

func (state *State) List() []model.CaptionStatus {
	state.mu.RLock()
	defer state.mu.RUnlock()
	items := make([]model.CaptionStatus, 0, len(state.statuses))
	for _, status := range state.statuses {
		items = append(items, status)
	}
	return items
}
