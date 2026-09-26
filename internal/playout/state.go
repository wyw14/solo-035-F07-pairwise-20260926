package playout

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type Output struct {
	EventID   model.EventID    `json:"event_id"`
	Revision  model.RevisionID `json:"revision"`
	Fence     uint64           `json:"fence"`
	Source    model.SourceID   `json:"source"`
	OnAir     bool             `json:"on_air"`
	Segment   string           `json:"segment"`
	UpdatedAt time.Time        `json:"updated_at"`
}

type State struct {
	mu      sync.RWMutex
	outputs map[model.EventID]Output
	trace   map[model.EventID][]string
}

func NewState() *State {
	return &State{outputs: make(map[model.EventID]Output), trace: make(map[model.EventID][]string)}
}

func (state *State) Stage(output Output) error {
	if !output.EventID.Valid() || !output.Revision.Valid() || !output.Source.Valid() {
		return fmt.Errorf("playout output identities are incomplete")
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	current := state.outputs[output.EventID]
	if current.Fence > output.Fence {
		return fmt.Errorf("playout output fence is stale")
	}
	output.OnAir = false
	state.outputs[output.EventID] = output
	state.trace[output.EventID] = append(state.trace[output.EventID], "staged:"+output.Segment)
	return nil
}

func (state *State) Publish(eventID model.EventID, revision model.RevisionID, fence uint64, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	output, ok := state.outputs[eventID]
	if !ok || output.Revision != revision || output.Fence != fence {
		return fmt.Errorf("playout publish does not match staged generation")
	}
	output.OnAir = true
	output.UpdatedAt = now.UTC()
	state.outputs[eventID] = output
	state.trace[eventID] = append(state.trace[eventID], "published:"+output.Segment)
	return nil
}

// PublishAndRetire performs the controlled cut inside the playout plane: the
// incoming staged generation goes on air while the outgoing session drops its
// output in the same critical section, so no instant exists with neither or
// both sessions driving the output.
func (state *State) PublishAndRetire(incoming model.EventID, revision model.RevisionID, fence uint64, outgoing model.EventID, now time.Time) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	next, ok := state.outputs[incoming]
	if !ok || next.Revision != revision || next.Fence != fence || next.OnAir {
		return fmt.Errorf("incoming playout generation is not staged for the cut")
	}
	previous, hadPrevious := state.outputs[outgoing]
	if outgoing.Valid() && (!hadPrevious || !previous.OnAir) {
		return fmt.Errorf("outgoing session is not holding the playout output")
	}
	next.OnAir = true
	next.UpdatedAt = now.UTC()
	state.outputs[incoming] = next
	state.trace[incoming] = append(state.trace[incoming], "published:"+next.Segment)
	if hadPrevious {
		previous.OnAir = false
		previous.UpdatedAt = now.UTC()
		state.outputs[outgoing] = previous
		state.trace[outgoing] = append(state.trace[outgoing], "retired:"+next.Segment)
	}
	return nil
}

// RevertCut restores the outgoing output after a failed cut, returning the
// incoming generation to its staged (not on air) state.
func (state *State) RevertCut(incoming, outgoing model.EventID, now time.Time) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if next, ok := state.outputs[incoming]; ok && next.OnAir {
		next.OnAir = false
		next.UpdatedAt = now.UTC()
		state.outputs[incoming] = next
		state.trace[incoming] = append(state.trace[incoming], "cut-reverted")
	}
	if previous, ok := state.outputs[outgoing]; ok && !previous.OnAir {
		previous.OnAir = true
		previous.UpdatedAt = now.UTC()
		state.outputs[outgoing] = previous
		state.trace[outgoing] = append(state.trace[outgoing], "output-restored")
	}
}

func (state *State) Stop(eventID model.EventID, now time.Time) {
	state.mu.Lock()
	defer state.mu.Unlock()
	output := state.outputs[eventID]
	output.OnAir = false
	output.UpdatedAt = now.UTC()
	state.outputs[eventID] = output
	state.trace[eventID] = append(state.trace[eventID], "stopped")
}

func (state *State) Output(eventID model.EventID) (Output, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	output, ok := state.outputs[eventID]
	return output, ok
}

func (state *State) Trace(eventID model.EventID) []string {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return append([]string(nil), state.trace[eventID]...)
}

func (state *State) List() []Output {
	state.mu.RLock()
	defer state.mu.RUnlock()
	items := make([]Output, 0, len(state.outputs))
	for _, output := range state.outputs {
		items = append(items, output)
	}
	return items
}
