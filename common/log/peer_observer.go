package log

import (
	"sync"
	"sync/atomic"
	"time"
)

// PeerSide identifies whether a peer belongs to an inbound or outbound
// connection.
type PeerSide string

const (
	PeerSideInbound  PeerSide = "inbound"
	PeerSideOutbound PeerSide = "outbound"
)

// PeerEvent describes a successfully established network peer connection.
// It intentionally contains no raw connection object so published events are
// immutable and safe to deliver asynchronously.
type PeerEvent struct {
	Time    time.Time
	Side    PeerSide
	Tag     string
	Network string
	Address AccessAddress
}

// PeerSubscription receives peer events for one side without blocking the
// connection handling path. Events are dropped when the subscription buffer is
// full.
type PeerSubscription struct {
	side    PeerSide
	events  chan PeerEvent
	dropped atomic.Uint64
	close   sync.Once
}

var peerSubscriptions = struct {
	sync.RWMutex
	items map[*PeerSubscription]struct{}
}{
	items: make(map[*PeerSubscription]struct{}),
}

// SubscribePeerEvents subscribes to peer events for side.
func SubscribePeerEvents(side PeerSide, bufferSize int) *PeerSubscription {
	if bufferSize < 1 {
		bufferSize = 1
	}
	subscription := &PeerSubscription{
		side:   side,
		events: make(chan PeerEvent, bufferSize),
	}
	peerSubscriptions.Lock()
	peerSubscriptions.items[subscription] = struct{}{}
	peerSubscriptions.Unlock()
	return subscription
}

// Events returns the subscription event stream.
func (s *PeerSubscription) Events() <-chan PeerEvent {
	return s.events
}

// Dropped returns the number of events dropped because the subscription buffer
// was full.
func (s *PeerSubscription) Dropped() uint64 {
	return s.dropped.Load()
}

// Close removes the subscription and closes its event stream.
func (s *PeerSubscription) Close() {
	s.close.Do(func() {
		peerSubscriptions.Lock()
		delete(peerSubscriptions.items, s)
		close(s.events)
		peerSubscriptions.Unlock()
	})
}

// PublishPeerEvent publishes an immutable peer event to matching subscribers.
func PublishPeerEvent(event PeerEvent) {
	peerSubscriptions.RLock()
	defer peerSubscriptions.RUnlock()

	if len(peerSubscriptions.items) == 0 {
		return
	}
	if event.Time.IsZero() {
		event.Time = time.Now()
	}
	for subscription := range peerSubscriptions.items {
		if subscription.side != event.Side {
			continue
		}
		select {
		case subscription.events <- event:
		default:
			subscription.dropped.Add(1)
		}
	}
}
