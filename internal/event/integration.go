package event

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type Integration struct {
	state *State
	clock func() time.Time
}

func NewIntegration(state *State, clock func() time.Time) *Integration {
	return &Integration{state: state, clock: clock}
}

func (integration *Integration) Get(id model.EventID) (model.LiveEvent, bool) {
	return integration.state.Get(id)
}

func (integration *Integration) Arm(id model.EventID) (model.LiveEvent, error) {
	event, ok := integration.state.Get(id)
	if !ok {
		return model.LiveEvent{}, fmt.Errorf("event %s not found", id)
	}
	if event.Phase == model.PhasePreparing {
		if _, err := integration.state.Transition(id, model.PhaseRehearsal, integration.clock()); err != nil {
			return model.LiveEvent{}, err
		}
	}
	return integration.state.Transition(id, model.PhaseArmed, integration.clock())
}

func (integration *Integration) MarkOnAir(id model.EventID) (model.LiveEvent, error) {
	return integration.state.Transition(id, model.PhaseOnAir, integration.clock())
}

func (integration *Integration) MarkFailedOver(id model.EventID) (model.LiveEvent, error) {
	return integration.state.Transition(id, model.PhaseFailedOver, integration.clock())
}

func (integration *Integration) Hold(id model.EventID) (model.LiveEvent, error) {
	return integration.state.Transition(id, model.PhaseHolding, integration.clock())
}

// Close ends an event after it has handed output to its successor.
func (integration *Integration) Close(id model.EventID) (model.LiveEvent, error) {
	return integration.state.Transition(id, model.PhaseClosed, integration.clock())
}

func (integration *Integration) CancelCommand(id model.EventID) (model.LiveEvent, error) {
	event, ok := integration.state.Get(id)
	if !ok {
		return model.LiveEvent{}, fmt.Errorf("event %s not found", id)
	}
	event.CommandEpoch++
	event.UpdatedAt = integration.clock().UTC()
	if event.Phase == model.PhaseFailedOver || event.Phase == model.PhaseOnAir {
		event.Phase = model.PhaseHolding
	}
	if err := integration.state.Update(event); err != nil {
		return model.LiveEvent{}, err
	}
	return event, nil
}
