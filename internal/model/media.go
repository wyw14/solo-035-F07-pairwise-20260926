package model

import "time"

type Frame struct {
	Source           SourceID  `json:"source"`
	SourceGeneration uint64    `json:"source_generation"`
	Sequence         int64     `json:"sequence"`
	Timecode         int64     `json:"timecode"`
	CapturedAt       time.Time `json:"captured_at"`
	VideoLocked      bool      `json:"video_locked"`
	AudioLocked      bool      `json:"audio_locked"`
}

type SignalStatus struct {
	Source         SourceID  `json:"source"`
	Generation     uint64    `json:"generation"`
	CurrentCommand CommandID `json:"current_command"`
	VideoLocked    bool      `json:"video_locked"`
	AudioLocked    bool      `json:"audio_locked"`
	LastFrame      int64     `json:"last_frame"`
	LastTimecode   int64     `json:"last_timecode"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type EncoderLease struct {
	ID         LeaseID    `json:"id"`
	EncoderID  string     `json:"encoder_id"`
	Owner      EventID    `json:"owner"`
	Purpose    string     `json:"purpose"`
	Fence      uint64     `json:"fence"`
	AcquiredAt time.Time  `json:"acquired_at"`
	ReleasedAt *time.Time `json:"released_at,omitempty"`
}

type AudioEvidence struct {
	ProbeID     string    `json:"probe_id"`
	Kind        string    `json:"kind"`
	Value       float64   `json:"value"`
	MeasuredAt  time.Time `json:"measured_at"`
	DerivedFrom string    `json:"derived_from,omitempty"`
}

type CaptionStatus struct {
	EventID    EventID    `json:"event_id"`
	Revision   RevisionID `json:"revision"`
	Continuous bool       `json:"continuous"`
	Closed     bool       `json:"closed"`
	EncoderErr string     `json:"encoder_error,omitempty"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

type Readiness struct {
	Ready        bool      `json:"ready"`
	VideoLocked  bool      `json:"video_locked"`
	AudioReady   bool      `json:"audio_ready"`
	CaptionReady bool      `json:"caption_ready"`
	Reason       string    `json:"reason"`
	CheckedAt    time.Time `json:"checked_at"`
}
