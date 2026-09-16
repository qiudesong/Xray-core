package log

import (
	"sync"
	"testing"
)

func TestPeerSubscriptionFiltersCopiesAndDrops(t *testing.T) {
	inbound := SubscribePeerEvents(PeerSideInbound, 1)
	outbound := SubscribePeerEvents(PeerSideOutbound, 1)
	t.Cleanup(inbound.Close)
	t.Cleanup(outbound.Close)

	event := PeerEvent{
		Side:    PeerSideInbound,
		Tag:     "inbound",
		Network: "tcp",
		Address: AccessAddress{Value: "203.0.113.10", Type: AccessAddressTypeIP},
	}
	PublishPeerEvent(event)
	event.Tag = "changed"
	PublishPeerEvent(event)

	got := <-inbound.Events()
	if got.Tag != "inbound" {
		t.Fatalf("event was not copied: got tag %q", got.Tag)
	}
	if dropped := inbound.Dropped(); dropped != 1 {
		t.Fatalf("inbound dropped = %d, want 1", dropped)
	}
	select {
	case got := <-outbound.Events():
		t.Fatalf("outbound subscription received inbound event: %+v", got)
	default:
	}
}

func TestPeerSubscriptionConcurrentPublishAndClose(t *testing.T) {
	subscription := SubscribePeerEvents(PeerSideInbound, 1)
	var publishers sync.WaitGroup
	for i := 0; i < 16; i++ {
		publishers.Add(1)
		go func() {
			defer publishers.Done()
			for j := 0; j < 100; j++ {
				PublishPeerEvent(PeerEvent{Side: PeerSideInbound})
			}
		}()
	}
	subscription.Close()
	publishers.Wait()
}
