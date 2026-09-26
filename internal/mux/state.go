package mux

import (
	"sort"
	"sync"

	"github.com/wyw14/solo-035/internal/model"
)

type State struct {
	mu       sync.RWMutex
	programs map[model.EventID]Program
	active   map[model.EventID]bool
	preview  map[model.EventID]bool
	onAir    model.EventID
}

func NewState() *State {
	return &State{
		programs: make(map[model.EventID]Program),
		active:   make(map[model.EventID]bool),
		preview:  make(map[model.EventID]bool),
	}
}

func (state *State) Put(program Program) error {
	if err := program.Validate(); err != nil {
		return err
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	state.programs[program.EventID] = program
	return nil
}

func (state *State) Activate(eventID model.EventID, revision model.RevisionID) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	program, ok := state.programs[eventID]
	if !ok || program.Revision != revision {
		return false
	}
	state.active[eventID] = true
	if state.onAir == "" {
		state.onAir = eventID
	}
	return true
}

func (state *State) Deactivate(eventID model.EventID) {
	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.active, eventID)
	if state.onAir == eventID {
		state.onAir = ""
	}
}

// MarkPreview flags a configured program as a director preview without making it
// the program output. Previews from adjacent live sessions may overlap.
func (state *State) MarkPreview(eventID model.EventID, revision model.RevisionID) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	program, ok := state.programs[eventID]
	if !ok || program.Revision != revision {
		return false
	}
	state.preview[eventID] = true
	return true
}

func (state *State) ClearPreview(eventID model.EventID) {
	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.preview, eventID)
}

func (state *State) Previewing(eventID model.EventID) bool {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.preview[eventID]
}

// Previews returns every event whose program is currently held on the
// director preview bus, independent of which one is on air.
func (state *State) Previews() []model.EventID {
	state.mu.RLock()
	defer state.mu.RUnlock()
	items := make([]model.EventID, 0, len(state.preview))
	for eventID := range state.preview {
		items = append(items, eventID)
	}
	sort.Slice(items, func(i, j int) bool { return items[i] < items[j] })
	return items
}

// PromoteToAir performs the controlled cut: the incoming program must still be
// on preview at its current revision, after which it becomes the single program
// output while the outgoing program is dropped from both buses atomically.
func (state *State) PromoteToAir(incoming, outgoing model.EventID, revision model.RevisionID) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	program, ok := state.programs[incoming]
	if !ok || program.Revision != revision || !state.preview[incoming] {
		return false
	}
	if state.onAir != "" && state.onAir != incoming {
		delete(state.active, state.onAir)
	}
	if outgoing.Valid() && outgoing != incoming {
		delete(state.active, outgoing)
		delete(state.preview, outgoing)
	}
	state.active[incoming] = true
	delete(state.preview, incoming)
	state.onAir = incoming
	return true
}

func (state *State) OnAirEvent() model.EventID {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.onAir
}

// RevertPromotion undoes a PromoteToAir when a later step of the cut fails:
// the incoming program returns to preview and the outgoing program regains the
// program output.
func (state *State) RevertPromotion(incoming, outgoing model.EventID) {
	state.mu.Lock()
	defer state.mu.Unlock()
	delete(state.active, incoming)
	state.preview[incoming] = true
	if outgoing.Valid() {
		state.active[outgoing] = true
		state.onAir = outgoing
	} else {
		state.onAir = ""
	}
}

func (state *State) Get(eventID model.EventID) (Program, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	program, ok := state.programs[eventID]
	return program, ok
}

func (state *State) List() []Program {
	state.mu.RLock()
	defer state.mu.RUnlock()
	items := make([]Program, 0, len(state.programs))
	for _, program := range state.programs {
		items = append(items, program)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.Before(items[j].CreatedAt) })
	return items
}
