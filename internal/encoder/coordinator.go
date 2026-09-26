package encoder

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type Coordinator struct {
	state *State
	clock func() time.Time
}

func NewCoordinator(state *State, clock func() time.Time) *Coordinator {
	return &Coordinator{state: state, clock: clock}
}

func (coordinator *Coordinator) ReserveBackup(owner model.EventID, profile string) (model.EncoderLease, error) {
	return coordinator.state.Acquire(owner, "backup", profile, coordinator.clock())
}

func (coordinator *Coordinator) ReservePrimary(owner model.EventID, profile string) (model.EncoderLease, error) {
	return coordinator.state.Acquire(owner, "primary", profile, coordinator.clock())
}

// ReservePreview binds an incoming event to a healthy encoder that no event
// currently owns, so its program can be configured for director preview while
// the outgoing event keeps broadcasting on its own encoder. The resources
// overlap only when a spare encoder exists; otherwise acquisition fails and the
// handover cannot be previewed yet.
func (coordinator *Coordinator) ReservePreview(owner model.EventID, profile string) (model.EncoderLease, error) {
	return coordinator.state.Acquire(owner, "preview", profile, coordinator.clock())
}

// LeaseFor reports the live lease owned by an event.
func (coordinator *Coordinator) LeaseFor(owner model.EventID) (model.EncoderLease, bool) {
	return coordinator.state.LeaseFor(owner)
}

// ReleaseFor releases whatever live lease an event still holds. It is used to
// settle the outgoing event's lease once the incoming event has taken output.
func (coordinator *Coordinator) ReleaseFor(owner model.EventID) (bool, error) {
	lease, ok := coordinator.state.LeaseFor(owner)
	if !ok {
		return false, nil
	}
	if err := coordinator.state.Release(lease, coordinator.clock()); err != nil {
		return false, err
	}
	return true, nil
}

func (coordinator *Coordinator) Start(lease model.EncoderLease) error {
	if !coordinator.state.Validate(lease) {
		return fmt.Errorf("encoder cannot start without its committed lease")
	}
	return nil
}

func (coordinator *Coordinator) Release(lease model.EncoderLease) error {
	return coordinator.state.Release(lease, coordinator.clock())
}

func (coordinator *Coordinator) Equipment() []Resource {
	return coordinator.state.List()
}

func (coordinator *Coordinator) SetHealth(id string, healthy bool) bool {
	return coordinator.state.SetHealth(id, healthy)
}
