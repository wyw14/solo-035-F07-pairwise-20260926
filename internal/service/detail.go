package service

import (
	"github.com/wyw14/solo-035/internal/audio"
	"github.com/wyw14/solo-035/internal/backup"
	"github.com/wyw14/solo-035/internal/caption"
	"github.com/wyw14/solo-035/internal/handoff"
	"github.com/wyw14/solo-035/internal/model"
	"github.com/wyw14/solo-035/internal/mux"
)

type SourceDetail struct {
	Source  model.SourceID `json:"source"`
	Frames  int            `json:"frames"`
	Dropped int64          `json:"dropped"`
}

type EventDetail struct {
	EventID      model.EventID         `json:"event_id"`
	Audio        *audio.PathState      `json:"audio,omitempty"`
	Caption      *caption.Cue          `json:"caption,omitempty"`
	Failover     *backup.FailoverState `json:"failover,omitempty"`
	Handover     *handoff.Handover     `json:"handover,omitempty"`
	PlayoutTrace []string              `json:"playout_trace"`
	WorkerActive bool                  `json:"worker_active"`
}

type OperationalDetail struct {
	Programs []mux.Program         `json:"programs"`
	Sources  []SourceDetail        `json:"sources"`
	Events   []EventDetail         `json:"events"`
	Captions []model.CaptionStatus `json:"captions"`
	Audio    []audio.PathState     `json:"audio_paths"`
}

func (runtime *Runtime) Detail() OperationalDetail {
	detail := OperationalDetail{
		Programs: runtime.Mux.Programs(),
		Captions: runtime.CaptionFlow.Snapshot(),
		Audio:    runtime.AudioState.List(),
	}
	for _, signal := range runtime.SourceFlow.Snapshot() {
		detail.Sources = append(detail.Sources, SourceDetail{
			Source:  signal.Source,
			Frames:  len(runtime.Sources.Frames(signal.Source)),
			Dropped: runtime.Receiver.Dropped(signal.Source),
		})
	}
	for _, item := range runtime.Events.List() {
		eventDetail := EventDetail{EventID: item.ID, PlayoutTrace: runtime.Playout.Trace(item.ID), WorkerActive: runtime.PlayoutFlow.Active(item.ID)}
		if path, ok := runtime.AudioState.Path(item.ID); ok {
			eventDetail.Audio = &path
		}
		if cue, ok := runtime.Continuity.Last(item.ID); ok {
			eventDetail.Caption = &cue
		}
		if failover, ok := runtime.Backup.Status(item.ID); ok {
			eventDetail.Failover = &failover
		}
		if hand, ok := runtime.Handoffs.PendingFor(item.ID); ok {
			eventDetail.Handover = &hand
		} else if hand, ok := runtime.Handoffs.PendingForIncoming(item.ID); ok {
			eventDetail.Handover = &hand
		}
		detail.Events = append(detail.Events, eventDetail)
	}
	return detail
}
