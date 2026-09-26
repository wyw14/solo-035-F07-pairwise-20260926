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

func (coordinator *Coordinator) LeaseFor(owner model.EventID) (model.EncoderLease, bool) {
	return coordinator.state.LeaseFor(owner)
}

func (coordinator *Coordinator) LeaseByID(id model.LeaseID) (model.EncoderLease, bool) {
	return coordinator.state.LeaseByID(id)
}

func (coordinator *Coordinator) SetHealth(id string, healthy bool) bool {
	return coordinator.state.SetHealth(id, healthy)
}
