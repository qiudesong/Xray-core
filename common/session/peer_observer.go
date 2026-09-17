package session

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/log"
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
	Address log.AccessAddress
}

// PeerSubscription receives peer events without blocking the connection handling
// path. Events are dropped when the subscription buffer is full.
type PeerSubscription struct {
	events          chan PeerEvent
	dropped         atomic.Uint64
	droppedInbound  atomic.Uint64
	droppedOutbound atomic.Uint64
	close           sync.Once
}

var peerSubscriptions = struct {
	sync.RWMutex
	items map[*PeerSubscription]struct{}
}{
	items: make(map[*PeerSubscription]struct{}),
}

// SubscribePeerEvents subscribes to inbound and outbound peer events.
func SubscribePeerEvents(bufferSize int) *PeerSubscription {
	if bufferSize < 1 {
		bufferSize = 1
	}
	subscription := &PeerSubscription{
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

// DroppedForSide returns the number of events dropped for one peer side.
func (s *PeerSubscription) DroppedForSide(side PeerSide) uint64 {
	switch side {
	case PeerSideInbound:
		return s.droppedInbound.Load()
	case PeerSideOutbound:
		return s.droppedOutbound.Load()
	default:
		return 0
	}
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

// PublishPeerEvent publishes an immutable peer event to all subscribers.
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
		select {
		case subscription.events <- event:
		default:
			subscription.dropped.Add(1)
			switch event.Side {
			case PeerSideInbound:
				subscription.droppedInbound.Add(1)
			case PeerSideOutbound:
				subscription.droppedOutbound.Add(1)
			}
		}
	}
}
