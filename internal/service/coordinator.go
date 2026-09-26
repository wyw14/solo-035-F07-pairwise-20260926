package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/caption"
	"github.com/wyw14/solo-035/internal/handoff"
	"github.com/wyw14/solo-035/internal/model"
)

type Coordinator struct {
	runtime *Runtime
}

func NewCoordinator(runtime *Runtime) *Coordinator {
	return &Coordinator{runtime: runtime}
}

func (coordinator *Coordinator) CreateEvent(name string) (model.LiveEvent, error) {
	event, err := coordinator.runtime.Events.Create(name, coordinator.runtime.Clock())
	if err != nil {
		return model.LiveEvent{}, err
	}
	if _, err := coordinator.runtime.EventFlow.Arm(event.ID); err != nil {
		return model.LiveEvent{}, err
	}
	return coordinator.runtime.Events.GetOrZero(event.ID), nil
}

func (coordinator *Coordinator) PrepareSignal(eventID model.EventID, sourceID model.SourceID) (model.SignalStatus, error) {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return model.SignalStatus{}, fmt.Errorf("event %s not found", eventID)
	}
	status := coordinator.runtime.SourceFlow.Switch(sourceID)
	event = event.NewSource(coordinator.runtime.Clock())
	event.SourceGeneration = status.Generation
	if err := coordinator.runtime.Events.Update(event); err != nil {
		return model.SignalStatus{}, err
	}
	coordinator.runtime.Quality.Reset(eventID)
	if err := coordinator.runtime.Sync.Open(eventID, 3); err != nil {
		return model.SignalStatus{}, err
	}
	return status, nil
}

func (coordinator *Coordinator) ConfirmSignal(sourceID model.SourceID, command model.CommandID) error {
	if err := coordinator.runtime.SourceFlow.Confirm(sourceID, command, true, true); err != nil {
		return err
	}
	if !coordinator.runtime.SourceFlow.Ready(sourceID) {
		return fmt.Errorf("source %s did not retain its current lock", sourceID)
	}
	return nil
}

func (coordinator *Coordinator) PushFrame(eventID model.EventID, frame model.Frame) error {
	received, err := coordinator.runtime.Receiver.Push(frame.Source, frame.SourceGeneration, frame.Sequence, frame.Timecode, frame.CapturedAt)
	if err != nil {
		return err
	}
	received.VideoLocked = frame.VideoLocked
	received.AudioLocked = frame.AudioLocked
	_, err = coordinator.runtime.Sync.Observe(eventID, received)
	return err
}

func (coordinator *Coordinator) ConfigureProgram(eventID model.EventID, sourceID model.SourceID) error {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return fmt.Errorf("event %s not found", eventID)
	}
	lease, err := coordinator.runtime.EncoderFlow.ReservePrimary(eventID, "1080p50")
	if err != nil {
		return err
	}
	if err := coordinator.runtime.EncoderFlow.Start(lease); err != nil {
		return err
	}
	program, err := coordinator.runtime.Mux.Configure(event, lease, 8000)
	if err != nil {
		return err
	}
	if err := coordinator.runtime.Mux.Activate(eventID, program.Revision); err != nil {
		return err
	}
	return coordinator.runtime.Playout.Stage(event, sourceID, lease.Fence, "current-program")
}

func (coordinator *Coordinator) EstablishInterlocks(eventID model.EventID) error {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return fmt.Errorf("event %s not found", eventID)
	}
	waveA, _, err := coordinator.runtime.AudioMonitor.Measure("probe-a", -18, coordinator.runtime.Clock())
	if err != nil {
		return err
	}
	waveB, _, err := coordinator.runtime.AudioMonitor.Measure("probe-b", -17, coordinator.runtime.Clock())
	if err != nil {
		return err
	}
	if err := coordinator.runtime.Evaluator.Accept(waveA); err != nil {
		return err
	}
	if err := coordinator.runtime.Evaluator.Accept(waveB); err != nil {
		return err
	}
	if err := coordinator.runtime.Quality.Record(eventID, waveA); err != nil {
		return err
	}
	if err := coordinator.runtime.Quality.Record(eventID, waveB); err != nil {
		return err
	}
	if err := coordinator.runtime.Continuity.Accept(eventID, caption.Cue{Revision: event.Revision, Sequence: 1, Text: "ON AIR"}); err != nil {
		return err
	}
	coordinator.runtime.CaptionFlow.Encode(eventID, event.Revision, "ON AIR")
	return nil
}

func (coordinator *Coordinator) CommitOnAir(eventID model.EventID, fence uint64) error {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return fmt.Errorf("event %s not found", eventID)
	}
	if !coordinator.runtime.Sync.Ready(eventID) {
		return fmt.Errorf("frame synchronization is not stable")
	}
	if err := coordinator.runtime.CaptionFlow.Ready(eventID, event.Revision); err != nil {
		return err
	}
	if _, err := coordinator.runtime.Quality.Permit(eventID, true, true); err != nil {
		return err
	}
	if err := coordinator.runtime.Playout.CommitOnAir(event, fence, map[string]any{"offset_ms": 0, "source_generation": event.SourceGeneration}); err != nil {
		return err
	}
	_, err := coordinator.runtime.EventFlow.MarkOnAir(eventID)
	return err
}

func (coordinator *Coordinator) ScheduleFailover(ctx context.Context, eventID model.EventID, delay time.Duration) (<-chan error, error) {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return nil, fmt.Errorf("event %s not found", eventID)
	}
	return coordinator.runtime.Backup.Schedule(ctx, event, "1080p50-backup", delay), nil
}

func (coordinator *Coordinator) CancelFailover(eventID model.EventID) error {
	if _, err := coordinator.runtime.EventFlow.CancelCommand(eventID); err != nil {
		return err
	}
	return coordinator.runtime.Backup.Cancel(eventID)
}

// PrepareHandover stands up a new, independently tracked incoming event while
// the outgoing event keeps broadcasting. The incoming event gets its own
// signal lock, stable frame window, caption chain and audio evidence, then
// overlaps onto a separate preview encoder with a staged (not published)
// program. batonAt is the requested moment of the controlled cut.
func (coordinator *Coordinator) PrepareHandover(outgoingID model.EventID, name string, sourceID model.SourceID, batonAt time.Time) (handoff.Handoff, model.LiveEvent, error) {
	outgoing, ok := coordinator.runtime.Events.Get(outgoingID)
	if !ok {
		return handoff.Handoff{}, model.LiveEvent{}, fmt.Errorf("outgoing event %s not found", outgoingID)
	}
	incoming, err := coordinator.CreateEvent(name)
	if err != nil {
		return handoff.Handoff{}, model.LiveEvent{}, err
	}
	if err := coordinator.primeIncoming(incoming, sourceID); err != nil {
		return handoff.Handoff{}, incoming, err
	}
	h, err := coordinator.runtime.Handoff.Preview(outgoing, incoming, sourceID, batonAt)
	if err != nil {
		return handoff.Handoff{}, incoming, err
	}
	return h, coordinator.runtime.Events.GetOrZero(incoming.ID), nil
}

// primeIncoming drives a freshly armed event through signal lock, frame
// stability and caption/audio interlocks so it is eligible to take the baton,
// without ever publishing output.
func (coordinator *Coordinator) primeIncoming(event model.LiveEvent, sourceID model.SourceID) error {
	status, err := coordinator.PrepareSignal(event.ID, sourceID)
	if err != nil {
		return err
	}
	if err := coordinator.ConfirmSignal(sourceID, status.CurrentCommand); err != nil {
		return err
	}
	base := coordinator.runtime.Clock().UTC()
	for sequence := int64(1); sequence <= 3; sequence++ {
		frame := model.Frame{
			Source:           sourceID,
			SourceGeneration: status.Generation,
			Sequence:         sequence,
			Timecode:         1000 + sequence,
			CapturedAt:       base.Add(time.Duration(sequence) * 20 * time.Millisecond),
			VideoLocked:      true,
			AudioLocked:      true,
		}
		if err := coordinator.PushFrame(event.ID, frame); err != nil {
			return err
		}
	}
	return coordinator.EstablishInterlocks(event.ID)
}

// ArmHandover re-runs the incoming event's readiness gate before the baton.
func (coordinator *Coordinator) ArmHandover(handoffID string) (handoff.Handoff, error) {
	return coordinator.runtime.Handoff.MarkReady(handoffID)
}

// ExecuteHandover performs the controlled cut now; when the incoming event is
// not ready the pass defers and the outgoing event keeps output.
func (coordinator *Coordinator) ExecuteHandover(handoffID string) (handoff.Handoff, error) {
	return coordinator.runtime.Handoff.Execute(handoffID)
}

// ScheduleHandover arms a timer for the handoff's requested baton moment. The
// timer only invokes the controlled cut, which re-checks the gate itself.
func (coordinator *Coordinator) ScheduleHandover(ctx context.Context, handoffID string) (<-chan error, handoff.Handoff, error) {
	h, ok := coordinator.runtime.Handoff.Get(handoffID)
	if !ok {
		return nil, handoff.Handoff{}, fmt.Errorf("handoff %s not found", handoffID)
	}
	worker := coordinator.runtime.HandoffSchedule.ScheduleAt(ctx, h.ID, h.BatonAt, coordinator.runtime.Clock(), func() error {
		_, err := coordinator.runtime.Handoff.Execute(h.ID)
		return err
	})
	return worker, h, nil
}

// RetryHandover re-arms a deferred pass with a new baton moment after the
// director has restored the incoming event's readiness.
func (coordinator *Coordinator) RetryHandover(handoffID string, batonAt time.Time) (handoff.Handoff, error) {
	return coordinator.runtime.Handoff.Retry(handoffID, batonAt)
}

// CancelHandover abandons a pass, stops its timer and releases the incoming
// event's preview lease. The outgoing event keeps broadcasting.
func (coordinator *Coordinator) CancelHandover(handoffID string) (handoff.Handoff, error) {
	if err := coordinator.runtime.HandoffSchedule.Cancel(handoffID); err != nil {
		// No live timer is fine; the handoff state is what matters.
		_ = err
	}
	return coordinator.runtime.Handoff.Cancel(handoffID, "operator cancelled handover")
}
