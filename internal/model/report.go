package model

import "time"

type OperationView struct {
	EventID          EventID    `json:"event_id"`
	Name             string     `json:"name"`
	Phase            EventPhase `json:"phase"`
	ProgramRevision  RevisionID `json:"program_revision"`
	SourceGeneration uint64     `json:"source_generation"`
	CommandEpoch     uint64     `json:"command_epoch"`
	Preview          bool       `json:"preview"`
	OnAir            bool       `json:"on_air"`
}

type HandoverView struct {
	Token      CommandID `json:"token"`
	Outgoing   EventID   `json:"outgoing_event_id"`
	Incoming   EventID   `json:"incoming_event_id"`
	TakeoverAt time.Time `json:"takeover_at"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason,omitempty"`
}

type EquipmentView struct {
	ID      string  `json:"id"`
	Kind    string  `json:"kind"`
	Owner   EventID `json:"owner,omitempty"`
	Fence   uint64  `json:"fence"`
	Healthy bool    `json:"healthy"`
}

type InterlockView struct {
	EventID EventID `json:"event_id"`
	Name    string  `json:"name"`
	Passed  bool    `json:"passed"`
	Detail  string  `json:"detail"`
}

type IncidentView struct {
	ID        string    `json:"id"`
	EventID   EventID   `json:"event_id"`
	Severity  string    `json:"severity"`
	Message   string    `json:"message"`
	CreatedAt time.Time `json:"created_at"`
}

type RuntimeSnapshot struct {
	CapturedAt time.Time       `json:"captured_at"`
	Operations []OperationView `json:"operations"`
	Equipment  []EquipmentView `json:"equipment"`
	Interlocks []InterlockView `json:"interlocks"`
	Incidents  []IncidentView  `json:"incidents"`
	Handovers  []HandoverView  `json:"handovers"`
}

func (snapshot RuntimeSnapshot) Healthy() bool {
	for _, equipment := range snapshot.Equipment {
		if !equipment.Healthy {
			return false
		}
	}
	for _, interlock := range snapshot.Interlocks {
		if !interlock.Passed {
			return false
		}
	}
	return true
}
