package mux

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

func (coordinator *Coordinator) Configure(event model.LiveEvent, lease model.EncoderLease, bitrate int) (Program, error) {
	if lease.Owner != event.ID {
		return Program{}, fmt.Errorf("encoder lease belongs to another event")
	}
	program, err := NewProgram(event.ID, event.Revision, lease.ID, bitrate, coordinator.clock())
	if err != nil {
		return Program{}, err
	}
	if err := coordinator.state.Put(program); err != nil {
		return Program{}, err
	}
	return program, nil
}

func (coordinator *Coordinator) Activate(eventID model.EventID, revision model.RevisionID) error {
	if !coordinator.state.Activate(eventID, revision) {
		return fmt.Errorf("program revision is not current")
	}
	return nil
}

func (coordinator *Coordinator) Stop(eventID model.EventID) {
	coordinator.state.Deactivate(eventID)
	coordinator.state.ClearPreview(eventID)
}

// Preview places a configured program on the director preview bus so an
// upcoming session can be checked while another session still owns the output.
func (coordinator *Coordinator) Preview(eventID model.EventID, revision model.RevisionID) error {
	if !coordinator.state.MarkPreview(eventID, revision) {
		return fmt.Errorf("program revision is not available for preview")
	}
	return nil
}

func (coordinator *Coordinator) ClearPreview(eventID model.EventID) {
	coordinator.state.ClearPreview(eventID)
}

func (coordinator *Coordinator) Previewing(eventID model.EventID) bool {
	return coordinator.state.Previewing(eventID)
}

func (coordinator *Coordinator) Previews() []model.EventID {
	return coordinator.state.Previews()
}

// PromoteToAir cuts the incoming preview program to air and drops the outgoing
// program from the program and preview buses in one guarded step.
func (coordinator *Coordinator) PromoteToAir(incoming, outgoing model.EventID, revision model.RevisionID) error {
	if !coordinator.state.PromoteToAir(incoming, outgoing, revision) {
		return fmt.Errorf("incoming program is not staged on preview at its current revision")
	}
	return nil
}

func (coordinator *Coordinator) OnAirEvent() model.EventID {
	return coordinator.state.OnAirEvent()
}

// RevertPromotion restores the outgoing program output after a cut fails late.
func (coordinator *Coordinator) RevertPromotion(incoming, outgoing model.EventID) {
	coordinator.state.RevertPromotion(incoming, outgoing)
}

func (coordinator *Coordinator) Programs() []Program {
	return coordinator.state.List()
}
