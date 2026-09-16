package log

import (
	"sync"
	"sync/atomic"
	"time"
)

// AccessEvent is a structured access log event captured when Record is called.
type AccessEvent struct {
	Time    time.Time
	Message AccessMessage
}

// AccessSubscription receives structured access events without blocking the
// connection handling path. Events are dropped when the subscription buffer is
// full.
type AccessSubscription struct {
	events  chan AccessEvent
	dropped atomic.Uint64
	close   sync.Once
}

var accessSubscriptions = struct {
	sync.RWMutex
	items map[*AccessSubscription]struct{}
}{
	items: make(map[*AccessSubscription]struct{}),
}

// SubscribeAccessEvents subscribes to structured access events.
func SubscribeAccessEvents(bufferSize int) *AccessSubscription {
	if bufferSize < 1 {
		bufferSize = 1
	}
	subscription := &AccessSubscription{
		events: make(chan AccessEvent, bufferSize),
	}
	accessSubscriptions.Lock()
	accessSubscriptions.items[subscription] = struct{}{}
	accessSubscriptions.Unlock()
	return subscription
}

// Events returns the subscription event stream.
func (s *AccessSubscription) Events() <-chan AccessEvent {
	return s.events
}

// Dropped returns the number of events dropped because the subscription buffer
// was full.
func (s *AccessSubscription) Dropped() uint64 {
	return s.dropped.Load()
}

// Close removes the subscription and closes its event stream.
func (s *AccessSubscription) Close() {
	s.close.Do(func() {
		accessSubscriptions.Lock()
		delete(accessSubscriptions.items, s)
		close(s.events)
		accessSubscriptions.Unlock()
	})
}

func publishAccessEvent(message *AccessMessage) {
	accessSubscriptions.RLock()
	defer accessSubscriptions.RUnlock()
	if len(accessSubscriptions.items) == 0 {
		return
	}

	messageCopy := *message
	event := AccessEvent{
		Time:    time.Now(),
		Message: messageCopy,
	}
	for subscription := range accessSubscriptions.items {
		select {
		case subscription.events <- event:
		default:
			subscription.dropped.Add(1)
		}
	}
}
