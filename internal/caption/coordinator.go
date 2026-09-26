package caption

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

func (coordinator *Coordinator) Encode(eventID model.EventID, revision model.RevisionID, payload string) model.CaptionStatus {
	if payload == "" {
		return coordinator.state.Failure(eventID, revision, "caption encoder received an empty cue", coordinator.clock())
	}
	return coordinator.state.Success(eventID, revision, coordinator.clock())
}

func (coordinator *Coordinator) ReportError(eventID model.EventID, revision model.RevisionID, message string) model.CaptionStatus {
	if message == "" {
		message = "caption encoder error"
	}
	return coordinator.state.Failure(eventID, revision, message, coordinator.clock())
}

func (coordinator *Coordinator) Ready(eventID model.EventID, revision model.RevisionID) error {
	status, ok := coordinator.state.Get(eventID)
	if !ok || status.Revision != revision {
		return fmt.Errorf("caption evidence does not match the current program")
	}
	if status.Closed {
		return fmt.Errorf("caption chain for event %s has been closed", eventID)
	}
	if status.EncoderErr != "" {
		return fmt.Errorf("caption encoder failed: %s", status.EncoderErr)
	}
	if !status.Continuous {
		return fmt.Errorf("caption continuity is not established")
	}
	return nil
}

// Close retires the caption chain attached to an event after a successful
// handover. It is a no-op when the event never established caption state.
func (coordinator *Coordinator) Close(eventID model.EventID) bool {
	_, ok := coordinator.state.Close(eventID, coordinator.clock())
	return ok
}

func (coordinator *Coordinator) Snapshot() []model.CaptionStatus {
	return coordinator.state.List()
}
