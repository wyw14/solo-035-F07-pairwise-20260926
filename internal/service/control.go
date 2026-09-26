package service

import (
	"context"
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/backup"
	"github.com/wyw14/solo-035/internal/model"
)

type ControlRequest struct {
	Action        string `json:"action"`
	EventID       string `json:"event_id"`
	SourceID      string `json:"source_id"`
	EncoderID     string `json:"encoder_id"`
	HandoverID    string `json:"handover_id"`
	IncomingName  string `json:"incoming_name"`
	Healthy       bool   `json:"healthy"`
	Bitrate       int    `json:"bitrate"`
	Count         int64  `json:"count"`
	Message       string `json:"message"`
	DelayMillis   int64  `json:"delay_millis"`
	BatonAtMillis int64  `json:"baton_at_millis"`
}

type ControlResult struct {
	Action string `json:"action"`
	Status string `json:"status"`
	Value  any    `json:"value,omitempty"`
}

func (runtime *Runtime) ExecuteControl(ctx context.Context, request ControlRequest) (ControlResult, error) {
	result := ControlResult{Action: request.Action, Status: "accepted"}
	eventID := model.EventID(request.EventID)
	sourceID := model.SourceID(request.SourceID)
	coordinator := NewCoordinator(runtime)
	switch request.Action {
	case "hold-event":
		value, err := runtime.EventFlow.Hold(eventID)
		result.Value = value
		return result, err
	case "mark-failed-over":
		value, err := runtime.EventFlow.MarkFailedOver(eventID)
		result.Value = value
		return result, err
	case "advance-program":
		value, err := runtime.Events.AdvanceProgram(eventID, runtime.Clock())
		result.Value = value
		return result, err
	case "advance-source":
		value, err := runtime.Events.AdvanceSource(eventID, runtime.Clock())
		result.Value = value
		return result, err
	case "retire-source":
		runtime.SourceFlow.Retire(sourceID)
		result.Value = sourceID
		return result, nil
	case "close-sync":
		runtime.Sync.Close(eventID)
		result.Value = eventID
		return result, nil
	case "set-encoder-health":
		if !runtime.EncoderFlow.SetHealth(request.EncoderID, request.Healthy) {
			return result, fmt.Errorf("encoder %s does not exist", request.EncoderID)
		}
		result.Value = map[string]any{"encoder_id": request.EncoderID, "healthy": request.Healthy}
		return result, nil
	case "adjust-bitrate":
		program, ok := runtime.MuxState.Get(eventID)
		if !ok {
			return result, fmt.Errorf("program for event %s does not exist", eventID)
		}
		updated, err := program.WithBitrate(request.Bitrate)
		if err != nil {
			return result, err
		}
		if err := runtime.MuxState.Put(updated); err != nil {
			return result, err
		}
		result.Value = updated
		return result, nil
	case "stop-program":
		runtime.Mux.Stop(eventID)
		runtime.Playout.Stop(eventID)
		result.Value = eventID
		return result, nil
	case "audio-failover":
		if err := runtime.Audio.Failover(eventID); err != nil {
			return result, err
		}
		result.Value, _ = runtime.AudioState.Path(eventID)
		return result, nil
	case "caption-error":
		event, ok := runtime.Events.Get(eventID)
		if !ok {
			return result, fmt.Errorf("event %s not found", eventID)
		}
		result.Value = runtime.CaptionFlow.ReportError(eventID, event.Revision, request.Message)
		return result, nil
	case "quality-reset":
		runtime.Quality.Reset(eventID)
		result.Value = eventID
		return result, nil
	case "record-gap":
		if err := runtime.Receiver.RecordGap(sourceID, request.Count); err != nil {
			return result, err
		}
		result.Value = map[string]any{"source": sourceID, "dropped": runtime.Receiver.Dropped(sourceID)}
		return result, nil
	case "schedule-failover":
		event, ok := runtime.Events.Get(eventID)
		if !ok {
			return result, fmt.Errorf("event %s not found", eventID)
		}
		decision, err := backup.Evaluate(event, 1, 0, runtime.Clock())
		if err != nil {
			return result, err
		}
		if err := decision.Validate(event); err != nil {
			return result, err
		}
		worker, err := coordinator.ScheduleFailover(ctx, eventID, time.Duration(request.DelayMillis)*time.Millisecond)
		if err != nil {
			return result, err
		}
		if err := <-worker; err != nil {
			return result, err
		}
		result.Value = decision
		return result, nil
	case "cancel-failover":
		if err := coordinator.CancelFailover(eventID); err != nil {
			return result, err
		}
		result.Value = eventID
		return result, nil
	case "recover-journal":
		report, err := runtime.Recover(func(model.EventID) model.SourceID {
			if sourceID.Valid() {
				return sourceID
			}
			return model.NewSourceID()
		})
		result.Value = report
		return result, err
	case "prepare-handover":
		incomingSource := sourceID
		if !incomingSource.Valid() {
			incomingSource = model.NewSourceID()
		}
		batonAt := handoverBatonAt(request, runtime.Clock)
		h, incoming, err := coordinator.PrepareHandover(eventID, request.IncomingName, incomingSource, batonAt)
		if err != nil {
			return result, err
		}
		result.Value = map[string]any{"handoff": h, "incoming_event": incoming}
		return result, nil
	case "arm-handover":
		value, err := coordinator.ArmHandover(request.HandoverID)
		result.Value = value
		return result, err
	case "execute-handover":
		value, err := coordinator.ExecuteHandover(request.HandoverID)
		result.Value = value
		return result, err
	case "schedule-handover":
		worker, h, err := coordinator.ScheduleHandover(ctx, request.HandoverID)
		if err != nil {
			return result, err
		}
		if err := <-worker; err != nil {
			// A deferred cut is an operational outcome, not a rejected command:
			// report the handoff state so the caller can see it held output.
			result.Status = "deferred"
		}
		final, _ := runtime.Handoff.Get(h.ID)
		result.Value = final
		return result, nil
	case "retry-handover":
		value, err := coordinator.RetryHandover(request.HandoverID, handoverBatonAt(request, runtime.Clock))
		result.Value = value
		return result, err
	case "cancel-handover":
		value, err := coordinator.CancelHandover(request.HandoverID)
		result.Value = value
		return result, err
	default:
		return result, fmt.Errorf("unsupported control action %q", request.Action)
	}
}

// handoverBatonAt resolves the requested cut moment: an absolute unix-millis
// timestamp wins, otherwise a relative delay from now, otherwise immediately.
func handoverBatonAt(request ControlRequest, clock func() time.Time) time.Time {
	now := clock().UTC()
	if request.BatonAtMillis > 0 {
		return time.UnixMilli(request.BatonAtMillis).UTC()
	}
	if request.DelayMillis > 0 {
		return now.Add(time.Duration(request.DelayMillis) * time.Millisecond)
	}
	return now
}
