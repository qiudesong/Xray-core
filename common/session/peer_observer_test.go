package session

import (
	"sync"
	"testing"

	"github.com/xtls/xray-core/common/log"
)

func TestPeerSubscriptionCopiesAndDrops(t *testing.T) {
	subscription := SubscribePeerEvents(1)
	t.Cleanup(subscription.Close)

	event := PeerEvent{
		Side:    PeerSideInbound,
		Tag:     "inbound",
		Network: "tcp",
		Address: log.AccessAddress{Value: "203.0.113.10", Type: log.AccessAddressTypeIP},
	}
	PublishPeerEvent(event)
	event.Tag = "changed"
	PublishPeerEvent(event)

	got := <-subscription.Events()
	if got.Tag != "inbound" {
		t.Fatalf("event was not copied: got tag %q", got.Tag)
	}
	if dropped := subscription.Dropped(); dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	if dropped := subscription.DroppedForSide(PeerSideInbound); dropped != 1 {
		t.Fatalf("inbound dropped = %d, want 1", dropped)
	}
}

func TestPeerSubscriptionConcurrentPublishAndClose(t *testing.T) {
	subscription := SubscribePeerEvents(1)
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

func TestPeerSubscriptionReceivesBothSidesAndTracksDrops(t *testing.T) {
	subscription := SubscribePeerEvents(1)
	t.Cleanup(subscription.Close)

	PublishPeerEvent(PeerEvent{Side: PeerSideInbound})
	PublishPeerEvent(PeerEvent{Side: PeerSideOutbound})
	if got := <-subscription.Events(); got.Side != PeerSideInbound {
		t.Fatalf("unexpected peer side: got %q, want %q", got.Side, PeerSideInbound)
	}
	if dropped := subscription.Dropped(); dropped != 1 {
		t.Fatalf("dropped = %d, want 1", dropped)
	}
	if dropped := subscription.DroppedForSide(PeerSideOutbound); dropped != 1 {
		t.Fatalf("outbound dropped = %d, want 1", dropped)
	}
	if dropped := subscription.DroppedForSide(PeerSideInbound); dropped != 0 {
		t.Fatalf("inbound dropped = %d, want 0", dropped)
	}
}
