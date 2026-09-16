package log

import "testing"

func TestAccessSubscriptionReceivesCopiesAndDropsWhenFull(t *testing.T) {
	subscription := SubscribeAccessEvents(1)
	t.Cleanup(subscription.Close)

	message := &AccessMessage{
		Status:      AccessAccepted,
		Email:       "first@example.com",
		InboundTag:  "inbound",
		OutboundTag: "outbound",
		Network:     "tcp",
		Destination: AccessAddress{Value: "example.com", Type: AccessAddressTypeDomain},
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
	message := &AccessMessage{
		From:        "source",
		To:          "target",
		Status:      AccessAccepted,
		Destination: AccessAddress{Value: "example.com", Type: AccessAddressTypeDomain},
	}
	if got, want := message.String(), "from source accepted target"; got != want {
		t.Fatalf("unexpected access log text: got %q, want %q", got, want)
	}
}
