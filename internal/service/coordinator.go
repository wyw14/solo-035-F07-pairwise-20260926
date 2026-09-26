package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/caption"
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
	return coordinator.configureProgram(eventID, sourceID, true)
}

// ConfigureProgramPreview reserves and stages an incoming session's encoder,
// program and playout generation without seizing the program output. It waits
// on the director preview bus until a handover cuts it to air.
func (coordinator *Coordinator) ConfigureProgramPreview(eventID model.EventID, sourceID model.SourceID) error {
	return coordinator.configureProgram(eventID, sourceID, false)
}

func (coordinator *Coordinator) configureProgram(eventID model.EventID, sourceID model.SourceID, takeOutput bool) error {
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
	if takeOutput {
		if err := coordinator.runtime.Mux.Activate(eventID, program.Revision); err != nil {
			return err
		}
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

// RehearseIncoming prepares a new live session for takeover while another
// session still holds the output: its signal, encoder, program and playout
// generation are established on preview and its interlocks satisfied, but the
// incoming event stays armed and never seizes the output.
func (coordinator *Coordinator) RehearseIncoming(eventID model.EventID, sourceID model.SourceID) error {
	event, ok := coordinator.runtime.Events.Get(eventID)
	if !ok {
		return fmt.Errorf("event %s not found", eventID)
	}
	status, err := coordinator.PrepareSignal(eventID, sourceID)
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
			Timecode:         2000 + sequence,
			CapturedAt:       base.Add(time.Duration(sequence) * 20 * time.Millisecond),
			VideoLocked:      true,
			AudioLocked:      true,
		}
		if err := coordinator.PushFrame(eventID, frame); err != nil {
			return err
		}
	}
	if err := coordinator.ConfigureProgramPreview(eventID, sourceID); err != nil {
		return err
	}
	if err := coordinator.EstablishInterlocks(eventID); err != nil {
		return err
	}
	event = coordinator.runtime.Events.GetOrZero(eventID)
	if event.Phase != model.PhaseArmed {
		return fmt.Errorf("incoming event %s must remain armed, is %s", eventID, event.Phase)
	}
	return nil
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
