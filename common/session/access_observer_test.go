package session

import (
	"testing"

	"github.com/xtls/xray-core/common/log"
)

func TestAccessSubscriptionReceivesCopiesAndDropsWhenFull(t *testing.T) {
	subscription := SubscribeAccessEvents(1)
	t.Cleanup(subscription.Close)

	message := &log.AccessMessage{
		Status:      log.AccessAccepted,
		Email:       "first@example.com",
		InboundTag:  "inbound",
		OutboundTag: "outbound",
		Network:     "tcp",
		Destination: log.AccessAddress{Value: "example.com", Type: log.AccessAddressTypeDomain},
	}
	publishAccessEvent(message)
	message.Email = "changed@example.com"
	publishAccessEvent(message)

	event := <-subscription.Events()
	if event.Message.Email != "first@example.com" {
		t.Fatalf("event was not copied: got %q", event.Message.Email)
	}
	if event.Message.Destination != message.Destination {
		t.Fatalf("structured destination was not copied: got %+v", event.Message.Destination)
	}
	if dropped := subscription.Dropped(); dropped != 1 {
		t.Fatalf("unexpected dropped event count: got %d, want 1", dropped)
	}
}

func TestAccessMessageStructuredMetadataDoesNotChangeText(t *testing.T) {
	message := &log.AccessMessage{
		From:        "source",
		To:          "target",
		Status:      log.AccessAccepted,
		Destination: log.AccessAddress{Value: "example.com", Type: log.AccessAddressTypeDomain},
	}
	if got, want := message.String(), "from source accepted target"; got != want {
		t.Fatalf("unexpected access log text: got %q, want %q", got, want)
	}
}

func TestLogRecordDoesNotPublishAccessEvent(t *testing.T) {
	subscription := SubscribeAccessEvents(1)
	t.Cleanup(subscription.Close)

	log.Record(&log.AccessMessage{Status: log.AccessAccepted})
	select {
	case event := <-subscription.Events():
		t.Fatalf("plain access log unexpectedly produced a session event: %+v", event)
	default:
	}
}
