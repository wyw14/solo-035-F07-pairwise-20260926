package handoff

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Scheduler fires the controlled cut at a handoff's baton time. Each pass gets
// its own timer; cancelling or completing a pass stops the timer. The actual
// cut always runs through Coordinator.Execute, which re-checks the gate, so a
// timer firing early or late never bypasses readiness.
type Scheduler struct {
	mu      sync.Mutex
	timers  map[string]context.CancelFunc
	results map[string]<-chan error
}

func NewScheduler() *Scheduler {
	return &Scheduler{timers: make(map[string]context.CancelFunc), results: make(map[string]<-chan error)}
}

// ScheduleAt runs execute at batonAt. A baton time in the past (or within the
// jitter window) fires immediately. Any previous timer for the same pass is
// replaced.
func (scheduler *Scheduler) ScheduleAt(parent context.Context, handoffID string, batonAt time.Time, now time.Time, execute func() error) <-chan error {
	delay := batonAt.UTC().Sub(now.UTC())
	if delay < 0 {
		delay = 0
	}

	scheduler.mu.Lock()
	if cancel := scheduler.timers[handoffID]; cancel != nil {
		cancel()
	}
	workerContext, cancel := context.WithCancel(parent)
	scheduler.timers[handoffID] = cancel
	result := make(chan error, 1)
	scheduler.results[handoffID] = result
	scheduler.mu.Unlock()

	go func() {
		defer close(result)
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-workerContext.Done():
			result <- workerContext.Err()
		case <-timer.C:
			result <- execute()
		}
	}()
	return result
}

// Cancel stops the timer for a pass without touching handoff state.
func (scheduler *Scheduler) Cancel(handoffID string) error {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	cancel := scheduler.timers[handoffID]
	if cancel == nil {
		return fmt.Errorf("no scheduled baton for handoff %s", handoffID)
	}
	cancel()
	delete(scheduler.timers, handoffID)
	delete(scheduler.results, handoffID)
	return nil
}

// Scheduled reports whether a timer is still armed for the pass.
func (scheduler *Scheduler) Scheduled(handoffID string) bool {
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	return scheduler.timers[handoffID] != nil
}
