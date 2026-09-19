package observatory

import (
	"context"
	"sync"
	"testing"
)

func TestObserverGetObservationReturnsDeepClone(t *testing.T) {
	observer := &Observer{
		status: []*OutboundStatus{{
			OutboundTag: "proxy",
			Alive:       true,
			Delay:       42,
		}},
	}

	message, err := observer.GetObservation(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	result, ok := message.(*ObservationResult)
	if !ok {
		t.Fatalf("expected *ObservationResult, got %T", message)
	}
	if len(result.Status) != 1 {
		t.Fatalf("expected 1 status, got %d", len(result.Status))
	}

	result.Status[0].Alive = false
	result.Status[0].Delay = 999

	if result.Status[0] == observer.status[0] {
		t.Fatal("expected returned status to be a deep clone")
	}
	if !observer.status[0].Alive {
		t.Fatal("mutating returned status changed observer state")
	}
	if observer.status[0].Delay != 42 {
		t.Fatalf("mutating returned status changed observer delay to %d", observer.status[0].Delay)
	}
}

func TestObserverGetObservationConcurrentWithUpdate(t *testing.T) {
	observer := &Observer{
		status: []*OutboundStatus{{OutboundTag: "proxy"}},
	}

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	go func() {
		defer waitGroup.Done()
		for i := 0; i < 1000; i++ {
			observer.updateStatusForResult("proxy", &ProbeResult{
				Alive: i%2 == 0,
				Delay: int64(i),
			})
		}
	}()
	go func() {
		defer waitGroup.Done()
		for i := 0; i < 1000; i++ {
			message, _ := observer.GetObservation(context.Background())
			status := message.(*ObservationResult).Status[0]
			_ = status.GetAlive()
			_ = status.GetDelay()
			_ = status.GetLastTryTime()
		}
	}()
	waitGroup.Wait()
}

func TestObserverUpdateStatusPrunesStaleOutbounds(t *testing.T) {
	observer := &Observer{
		status: []*OutboundStatus{
			{
				OutboundTag:     "keep",
				Alive:           true,
				Delay:           42,
				LastErrorReason: "",
				LastSeenTime:    111,
				LastTryTime:     222,
			},
			{
				OutboundTag:     "drop",
				Alive:           false,
				Delay:           99999999,
				LastErrorReason: "probe failed",
				LastSeenTime:    333,
				LastTryTime:     444,
			},
		},
	}

	observer.clearRemovedOutbounds([]string{"keep"})

	if len(observer.status) != 1 {
		t.Fatalf("expected 1 status after pruning, got %d", len(observer.status))
	}

	got := observer.status[0]
	if got.OutboundTag != "keep" {
		t.Fatalf("expected remaining status for keep, got %q", got.OutboundTag)
	}
	if !got.Alive {
		t.Fatal("expected remaining status to preserve Alive field")
	}
	if got.Delay != 42 {
		t.Fatalf("expected remaining status to preserve Delay, got %d", got.Delay)
	}
	if got.LastSeenTime != 111 {
		t.Fatalf("expected remaining status to preserve LastSeenTime, got %d", got.LastSeenTime)
	}
	if got.LastTryTime != 222 {
		t.Fatalf("expected remaining status to preserve LastTryTime, got %d", got.LastTryTime)
	}
}

func TestObserverUpdateStatusClearsWhenNoOutboundsRemain(t *testing.T) {
	observer := &Observer{
		status: []*OutboundStatus{
			{OutboundTag: "drop-1"},
			{OutboundTag: "drop-2"},
		},
	}

	observer.clearRemovedOutbounds(nil)

	if len(observer.status) != 0 {
		t.Fatalf("expected all statuses to be removed, got %d", len(observer.status))
	}
}
