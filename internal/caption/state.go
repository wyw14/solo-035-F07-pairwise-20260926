package caption

import (
	"sync"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type State struct {
	mu       sync.RWMutex
	statuses map[model.EventID]model.CaptionStatus
	retired  map[model.EventID]time.Time
}

func NewState() *State {
	return &State{statuses: make(map[model.EventID]model.CaptionStatus), retired: make(map[model.EventID]time.Time)}
}

func (state *State) Update(status model.CaptionStatus) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if _, ok := state.retired[status.EventID]; ok {
		return
	}
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

// Retire closes the caption chain for an event that has handed its output away.
// Later statuses for the same event are ignored so a stale encoder or cue cannot
// resurrect a finished session's caption state.
func (state *State) Retire(eventID model.EventID, now time.Time) {
	state.mu.Lock()
	defer state.mu.Unlock()
	state.retired[eventID] = now.UTC()
}

func (state *State) RetiredAt(eventID model.EventID) (time.Time, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	retiredAt, ok := state.retired[eventID]
	return retiredAt, ok
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
