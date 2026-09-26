package encoder

import (
	"sort"
	"sync"

	"github.com/wyw14/solo-035/internal/model"
)

type Resource struct {
	ID      string             `json:"id"`
	Profile string             `json:"profile"`
	Healthy bool               `json:"healthy"`
	Lease   model.EncoderLease `json:"lease"`
}

type State struct {
	mu        sync.RWMutex
	resources map[string]Resource
	fences    map[string]uint64
}

func NewState(ids ...string) *State {
	state := &State{resources: make(map[string]Resource), fences: make(map[string]uint64)}
	for _, id := range ids {
		state.resources[id] = Resource{ID: id, Healthy: true}
	}
	return state
}

func (state *State) List() []Resource {
	state.mu.RLock()
	defer state.mu.RUnlock()
	items := make([]Resource, 0, len(state.resources))
	for _, resource := range state.resources {
		items = append(items, resource)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ID < items[j].ID })
	return items
}

func (state *State) SetHealth(id string, healthy bool) bool {
	state.mu.Lock()
	defer state.mu.Unlock()
	resource, ok := state.resources[id]
	if !ok {
		return false
	}
	resource.Healthy = healthy
	state.resources[id] = resource
	return true
}

func (state *State) Owner(id string) model.EventID {
	state.mu.RLock()
	defer state.mu.RUnlock()
	return state.resources[id].Lease.Owner
}

// LeaseFor returns the live lease currently held by an event, if any. It lets
// handover cleanup target the specific lease that belongs to the outgoing event
// without guessing which encoder resource the event bound to.
func (state *State) LeaseFor(owner model.EventID) (model.EncoderLease, bool) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	for _, resource := range state.resources {
		if resource.Lease.Owner == owner && resource.Lease.ReleasedAt == nil {
			return resource.Lease, true
		}
	}
	return model.EncoderLease{}, false
}

func (state *State) nextFence(id string) uint64 {
	state.fences[id]++
	return state.fences[id]
}
