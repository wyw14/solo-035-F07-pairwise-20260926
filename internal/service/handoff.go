package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/handoff"
	"github.com/wyw14/solo-035/internal/model"
)

// PrepareHandover stages an incoming session on preview alongside the live
// outgoing session and records the designated takeover instant.
func (coordinator *Coordinator) PrepareHandover(outgoingID, incomingID model.EventID, delay time.Duration) (handoff.Handover, error) {
	takeoverAt := coordinator.runtime.Clock().Add(delay)
	return coordinator.runtime.HandoverFlow.Prepare(outgoingID, incomingID, takeoverAt)
}

// ExecuteHandover fires the controlled cut for a prepared handover. If the
// incoming session is not ready the handover aborts and the outgoing session
// keeps the output.
func (coordinator *Coordinator) ExecuteHandover(token model.CommandID) (handoff.Handover, error) {
	return coordinator.runtime.HandoverFlow.Execute(token)
}

// ScheduleHandover defers the cut to its recorded takeover instant.
func (coordinator *Coordinator) ScheduleHandover(ctx context.Context, token model.CommandID) (<-chan error, error) {
	handover, ok := coordinator.runtime.HandoverFlow.Get(token)
	if !ok {
		return nil, fmt.Errorf("handover %s does not exist", token)
	}
	delay := time.Until(handover.TakeoverAt)
	if delay < 0 {
		delay = 0
	}
	return coordinator.runtime.HandoverFlow.Schedule(ctx, token, delay), nil
}

// CancelHandover withdraws a prepared handover and releases the incoming
// session's preview resources without changing the outgoing output.
func (coordinator *Coordinator) CancelHandover(token model.CommandID) (handoff.Handover, error) {
	return coordinator.runtime.HandoverFlow.CancelWithdraw(token)
}

func (runtime *Runtime) Handovers() []handoff.Handover {
	return runtime.HandoverFlow.List()
}
