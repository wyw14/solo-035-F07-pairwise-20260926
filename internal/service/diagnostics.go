package service

import (
	"fmt"
	"sort"
	"time"

	"github.com/wyw14/solo-035/internal/model"
)

type DiagnosticCheck struct {
	Name      string    `json:"name"`
	Passed    bool      `json:"passed"`
	Detail    string    `json:"detail"`
	CheckedAt time.Time `json:"checked_at"`
}

type DiagnosticReport struct {
	Healthy   bool              `json:"healthy"`
	Checks    []DiagnosticCheck `json:"checks"`
	CheckedAt time.Time         `json:"checked_at"`
}

func (report DiagnosticReport) Failures() []DiagnosticCheck {
	items := make([]DiagnosticCheck, 0)
	for _, check := range report.Checks {
		if !check.Passed {
			items = append(items, check)
		}
	}
	return items
}

func (runtime *Runtime) Diagnostics() DiagnosticReport {
	now := runtime.Clock().UTC()
	checks := []DiagnosticCheck{
		runtime.checkEvents(now),
		runtime.checkSignals(now),
		runtime.checkEncoders(now),
		runtime.checkPlayout(now),
		runtime.checkJournal(now),
		runtime.checkInterlocks(now),
		runtime.checkHandovers(now),
	}
	sort.Slice(checks, func(i, j int) bool { return checks[i].Name < checks[j].Name })
	report := DiagnosticReport{Healthy: true, Checks: checks, CheckedAt: now}
	for _, check := range checks {
		if !check.Passed {
			report.Healthy = false
		}
	}
	return report
}

func (runtime *Runtime) checkEvents(now time.Time) DiagnosticCheck {
	events := runtime.Events.List()
	check := DiagnosticCheck{Name: "event-generations", Passed: true, CheckedAt: now}
	if len(events) == 0 {
		check.Detail = "runtime is ready for the first event"
		return check
	}
	for _, event := range events {
		if !event.ID.Valid() || !event.Revision.Valid() {
			check.Passed = false
			check.Detail = fmt.Sprintf("event %s has an invalid identity", event.ID)
			return check
		}
		if event.CommandEpoch == 0 || event.SourceGeneration == 0 {
			check.Passed = false
			check.Detail = fmt.Sprintf("event %s has an empty command or source generation", event.ID)
			return check
		}
	}
	check.Detail = fmt.Sprintf("%d event generations are internally consistent", len(events))
	return check
}

func (runtime *Runtime) checkSignals(now time.Time) DiagnosticCheck {
	signals := runtime.SourceFlow.Snapshot()
	check := DiagnosticCheck{Name: "signal-locks", Passed: true, CheckedAt: now}
	if len(signals) == 0 {
		check.Detail = "no signal receivers are registered"
		return check
	}
	locked := 0
	for _, signal := range signals {
		if signal.Generation == 0 || !signal.Source.Valid() {
			check.Passed = false
			check.Detail = "a signal receiver has invalid generation metadata"
			return check
		}
		if signal.VideoLocked && signal.AudioLocked {
			locked++
		}
	}
	if locked == 0 {
		check.Passed = false
		check.Detail = "no registered signal has complete media lock"
		return check
	}
	check.Detail = fmt.Sprintf("%d of %d signals have complete media lock", locked, len(signals))
	return check
}

func (runtime *Runtime) checkEncoders(now time.Time) DiagnosticCheck {
	resources := runtime.EncoderFlow.Equipment()
	check := DiagnosticCheck{Name: "encoder-leases", Passed: true, CheckedAt: now}
	thresholds := runtime.Evaluator.Thresholds()
	if !runtime.AudioMonitor.WithinRange(thresholds.MinimumLoudness) || !runtime.AudioMonitor.WithinRange(thresholds.MaximumLoudness) {
		check.Passed = false
		check.Detail = "audio and quality thresholds disagree"
		return check
	}
	owners := make(map[model.EventID]string)
	healthy := 0
	for _, resource := range resources {
		if resource.Healthy {
			healthy++
		}
		if resource.Lease.Owner == "" {
			continue
		}
		if runtime.Encoders.Owner(resource.ID) != resource.Lease.Owner {
			check.Passed = false
			check.Detail = fmt.Sprintf("encoder %s owner view is inconsistent", resource.ID)
			return check
		}
		if previous := owners[resource.Lease.Owner]; previous != "" {
			check.Passed = false
			check.Detail = fmt.Sprintf("event %s owns both %s and %s", resource.Lease.Owner, previous, resource.ID)
			return check
		}
		if resource.Lease.Fence == 0 || !resource.Lease.ID.Valid() {
			check.Passed = false
			check.Detail = fmt.Sprintf("encoder %s has an invalid lease fence", resource.ID)
			return check
		}
		owners[resource.Lease.Owner] = resource.ID
	}
	if healthy == 0 {
		check.Passed = false
		check.Detail = "no encoder resource is healthy"
		return check
	}
	check.Detail = fmt.Sprintf("%d encoders are healthy and %d leases are fenced", healthy, len(owners))
	return check
}

func (runtime *Runtime) checkPlayout(now time.Time) DiagnosticCheck {
	outputs := runtime.PlayoutState.List()
	check := DiagnosticCheck{Name: "playout-generations", Passed: true, CheckedAt: now}
	for _, output := range outputs {
		if !output.EventID.Valid() || !output.Revision.Valid() || !output.Source.Valid() {
			check.Passed = false
			check.Detail = "a playout output has incomplete generation identity"
			return check
		}
		if output.OnAir && output.Fence == 0 {
			check.Passed = false
			check.Detail = fmt.Sprintf("on-air output %s has no fencing value", output.EventID)
			return check
		}
	}
	check.Detail = fmt.Sprintf("%d playout outputs retain revision and source identity", len(outputs))
	return check
}

func (runtime *Runtime) checkJournal(now time.Time) DiagnosticCheck {
	records := runtime.Journal.Records()
	check := DiagnosticCheck{Name: "journal-order", Passed: true, CheckedAt: now}
	proofs := make(map[model.EventID]bool)
	for _, record := range records {
		var payload map[string]any
		if err := record.Decode(&payload); err != nil {
			check.Passed = false
			check.Detail = fmt.Sprintf("journal record %s cannot be decoded", record.ID)
			return check
		}
		if record.Kind == "lip-sync-proof" {
			proofs[record.EventID] = true
		}
		if record.Kind == "on-air" && !proofs[record.EventID] {
			check.Passed = false
			check.Detail = fmt.Sprintf("event %s was published before its lip-sync proof", record.EventID)
			return check
		}
	}
	for _, operation := range runtime.Operations() {
		revision, fence, known := runtime.JournalState.Program(operation.EventID)
		if known && (revision != operation.ProgramRevision || fence == 0) {
			check.Passed = false
			check.Detail = fmt.Sprintf("journal program generation for %s is inconsistent", operation.EventID)
			return check
		}
	}
	check.Detail = fmt.Sprintf("%d durable records at %s preserve proof-before-publication order", len(records), runtime.JournalStore.Path())
	return check
}

func (runtime *Runtime) checkInterlocks(now time.Time) DiagnosticCheck {
	interlocks := runtime.Interlocks()
	check := DiagnosticCheck{Name: "on-air-interlocks", Passed: true, CheckedAt: now}
	operations := runtime.Operations()
	active := make(map[model.EventID]bool)
	for _, operation := range operations {
		if operation.Phase == model.PhaseOnAir || operation.Phase == model.PhaseFailedOver {
			active[operation.EventID] = true
		}
	}
	for _, interlock := range interlocks {
		if active[interlock.EventID] && !interlock.Passed {
			check.Passed = false
			check.Detail = fmt.Sprintf("active event %s failed %s", interlock.EventID, interlock.Name)
			return check
		}
	}
	check.Detail = fmt.Sprintf("%d interlock observations match active event state", len(interlocks))
	return check
}

func (runtime *Runtime) checkHandovers(now time.Time) DiagnosticCheck {
	check := DiagnosticCheck{Name: "event-handovers", Passed: true, CheckedAt: now}
	handovers := runtime.HandoverFlow.List()
	pending := 0
	expectedPreview := make(map[model.EventID]bool)
	var latestCompleted model.EventID
	for _, handover := range handovers {
		switch handover.Status {
		case "previewing":
			pending++
			expectedPreview[handover.Incoming] = true
			if runtime.Mux.OnAirEvent() != handover.Outgoing {
				check.Passed = false
				check.Detail = fmt.Sprintf("pending handover %s lost its outgoing output", handover.Token)
				return check
			}
		case "aborted":
			// The cut never happened: the incoming session stays under director
			// preview awaiting a later attempt, while the outgoing session kept air.
			expectedPreview[handover.Incoming] = true
		case "on-air":
			latestCompleted = handover.Incoming
		}
	}
	actualPreview := make(map[model.EventID]bool)
	for _, eventID := range runtime.Mux.Previews() {
		actualPreview[eventID] = true
	}
	for eventID := range expectedPreview {
		if !actualPreview[eventID] {
			check.Passed = false
			check.Detail = fmt.Sprintf("incoming session %s lost its director preview", eventID)
			return check
		}
	}
	for eventID := range actualPreview {
		if !expectedPreview[eventID] {
			check.Passed = false
			check.Detail = fmt.Sprintf("session %s holds an orphan preview", eventID)
			return check
		}
	}
	if latestCompleted != "" && runtime.Mux.OnAirEvent() != latestCompleted && pending == 0 {
		// A later pending handover may legitimately be about to move the output.
		check.Passed = false
		check.Detail = fmt.Sprintf("latest completed handover expected %s on air", latestCompleted)
		return check
	}
	check.Detail = fmt.Sprintf("%d handovers recorded, %d awaiting their takeover instant", len(handovers), pending)
	return check
}
