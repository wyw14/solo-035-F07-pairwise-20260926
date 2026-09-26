package handoff

import (
	"fmt"
)

type RecoveryRecord struct {
	Handover Handover `json:"handover"`
}

func (state *State) ExportRecovery() []RecoveryRecord {
	handovers := state.List()
	records := make([]RecoveryRecord, 0, len(handovers))
	for _, handover := range handovers {
		records = append(records, RecoveryRecord{Handover: handover})
	}
	return records
}

func (state *State) RestoreRecovery(records []RecoveryRecord) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	for _, record := range records {
		handover := record.Handover
		if !handover.Token.Valid() || !handover.Outgoing.Valid() || !handover.Incoming.Valid() {
			return fmt.Errorf("handoff recovery identity is invalid")
		}
		if handover.Status == "" {
			return fmt.Errorf("handoff %s has incomplete recovery state", handover.Token)
		}
		state.handovers[handover.Token] = handover
	}
	return nil
}
