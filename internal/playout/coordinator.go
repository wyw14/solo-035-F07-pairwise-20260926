package playout

import (
	"fmt"
	"time"

	"github.com/wyw14/solo-035/internal/journal"
	"github.com/wyw14/solo-035/internal/model"
)

type Coordinator struct {
	state   *State
	journal *journal.Integration
	clock   func() time.Time
}

func NewCoordinator(state *State, log *journal.Integration, clock func() time.Time) *Coordinator {
	return &Coordinator{state: state, journal: log, clock: clock}
}

func (coordinator *Coordinator) Stage(event model.LiveEvent, source model.SourceID, fence uint64, segment string) error {
	return coordinator.state.Stage(Output{
		EventID:   event.ID,
		Revision:  event.Revision,
		Fence:     fence,
		Source:    source,
		Segment:   segment,
		UpdatedAt: coordinator.clock().UTC(),
	})
}

func (coordinator *Coordinator) CommitOnAir(event model.LiveEvent, fence uint64, lipSync map[string]any) error {
	if _, err := coordinator.journal.Persist(event.ID, "lip-sync-proof", event.Revision, fence, lipSync); err != nil {
		return err
	}
	if err := coordinator.state.Publish(event.ID, event.Revision, fence, coordinator.clock()); err != nil {
		return err
	}
	if _, err := coordinator.journal.Persist(event.ID, "on-air", event.Revision, fence, map[string]any{"published": true}); err != nil {
		coordinator.state.Stop(event.ID, coordinator.clock())
		return fmt.Errorf("on-air state was not durable: %w", err)
	}
	return nil
}

// Cut performs the handover switch in the playout plane: the incoming staged
// generation is durably proven and published while the outgoing generation is
// retired in the same step. If durability fails the outgoing output is restored
// so the audience never follows an unproven cut.
func (coordinator *Coordinator) Cut(incoming, outgoing model.LiveEvent, fence uint64, lipSync map[string]any) error {
	if _, err := coordinator.journal.Persist(incoming.ID, "lip-sync-proof", incoming.Revision, fence, lipSync); err != nil {
		return err
	}
	if err := coordinator.state.PublishAndRetire(incoming.ID, incoming.Revision, fence, outgoing.ID, coordinator.clock()); err != nil {
		return err
	}
	if _, err := coordinator.journal.Persist(incoming.ID, "on-air", incoming.Revision, fence, map[string]any{"published": true, "took_over_from": outgoing.ID}); err != nil {
		coordinator.state.RevertCut(incoming.ID, outgoing.ID, coordinator.clock())
		return fmt.Errorf("handover on-air state was not durable: %w", err)
	}
	return nil
}

func (coordinator *Coordinator) Stop(eventID model.EventID) {
	coordinator.state.Stop(eventID, coordinator.clock())
}

// RevertCut undoes a Cut when a later step of the handover fails.
func (coordinator *Coordinator) RevertCut(incoming, outgoing model.EventID) {
	coordinator.state.RevertCut(incoming, outgoing, coordinator.clock())
}

func (coordinator *Coordinator) Output(eventID model.EventID) (Output, bool) {
	return coordinator.state.Output(eventID)
}

func (coordinator *Coordinator) Trace(eventID model.EventID) []string {
	return coordinator.state.Trace(eventID)
}
