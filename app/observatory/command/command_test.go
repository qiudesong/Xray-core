package command

import (
	"context"
	"testing"
	"time"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/features"
	"github.com/xtls/xray-core/features/extension"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type snapshotObservatory struct {
	snapshots []extension.ProbeSnapshot
	tags      map[string]struct{}
	legacy    *observatory.ObservationResult
}

func (*snapshotObservatory) Type() interface{} { return extension.ObservatoryType() }
func (*snapshotObservatory) Start() error      { return nil }
func (*snapshotObservatory) Close() error      { return nil }
func (o *snapshotObservatory) GetObservation(context.Context) (proto.Message, error) {
	if o.legacy == nil {
		return &observatory.ObservationResult{}, nil
	}
	return o.legacy, nil
}

func (o *snapshotObservatory) GetProbeSnapshots(_ context.Context, filter extension.ProbeSnapshotFilter) ([]extension.ProbeSnapshot, error) {
	var result []extension.ProbeSnapshot
	for _, snapshot := range o.snapshots {
		if filter.ProbeTag != "" && snapshot.ProbeTag != filter.ProbeTag {
			continue
		}
		if filter.OutboundTag != "" && snapshot.OutboundTag != filter.OutboundTag {
			continue
		}
		result = append(result, snapshot)
	}
	return result, nil
}

func (o *snapshotObservatory) GetFeaturesByTag(tag string) (features.Feature, error) {
	if _, found := o.tags[tag]; !found {
		return nil, status.Error(codes.NotFound, "not found")
	}
	return o, nil
}

func (o *snapshotObservatory) GetFeaturesTag() []string {
	result := make([]string, 0, len(o.tags))
	for tag := range o.tags {
		result = append(result, tag)
	}
	return result
}

func TestGetOutboundStatusKeepsLegacyBehavior(t *testing.T) {
	legacy := &observatory.ObservationResult{Status: []*observatory.OutboundStatus{{OutboundTag: "x", Alive: true}}}
	response, err := (&service{observatory: &snapshotObservatory{legacy: legacy}}).GetOutboundStatus(context.Background(), &GetOutboundStatusRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != legacy {
		t.Fatalf("legacy status = %#v, want original observation %#v", response.Status, legacy)
	}
}

func TestListProbeStatusesGroupsAndSortsByObserver(t *testing.T) {
	checkedAt := time.Unix(1700000000, 0)
	locationObservedAt := time.Unix(1700000001, 0)
	provider := &snapshotObservatory{
		tags: map[string]struct{}{"a": {}, "b": {}},
		legacy: &observatory.ObservationResult{Status: []*observatory.OutboundStatus{
			{OutboundTag: "x", Alive: true},
			{OutboundTag: "y", Alive: false},
		}},
		snapshots: []extension.ProbeSnapshot{
			{
				ProbeTag: "b", OutboundTag: "x", Method: "ip", EffectiveState: extension.ProbeStateHealthy,
				LastLocation: extension.ProbeLocation{Source: "cloudflare", Country: "US", ObservedAt: locationObservedAt},
			},
			{
				ProbeTag: "a", OutboundTag: "y", Method: "http", PolicyState: extension.ProbeStateUnhealthy,
				EffectiveState: extension.ProbeStateStale, Latest: extension.ProbeSample{CheckedAt: checkedAt, HTTPStatus: 503},
			},
			{ProbeTag: "a", OutboundTag: "x", Method: "http", EffectiveState: extension.ProbeStateHealthy},
		},
	}
	response, err := (&service{observatory: provider}).ListProbeStatuses(context.Background(), &ListProbeStatusesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Observers) != 2 || response.Observers[0].ObserverTag != "a" || response.Observers[1].ObserverTag != "b" {
		t.Fatalf("unexpected observer groups: %#v", response.Observers)
	}
	if response.Observers[0].Status[0].OutboundTag != "x" || response.Observers[0].Status[1].OutboundTag != "y" {
		t.Fatalf("observer statuses are not sorted: %#v", response.Observers[0].Status)
	}
	if response.Observers[0].Status[1].EffectiveState != "stale" || response.Observers[0].Status[1].CheckedAt != checkedAt.Unix() {
		t.Fatalf("snapshot fields were not preserved: %#v", response.Observers[0].Status[1])
	}
	location := response.Observers[1].Status[0]
	if location.LocationSource != "cloudflare" || location.Country != "US" || location.LocationObservedAt != locationObservedAt.Unix() {
		t.Fatalf("location fields were not preserved: %#v", location)
	}
}

func TestListProbeStatusesFiltersObserverAndOutbound(t *testing.T) {
	provider := &snapshotObservatory{
		tags: map[string]struct{}{"health": {}},
		legacy: &observatory.ObservationResult{Status: []*observatory.OutboundStatus{
			{OutboundTag: "x"},
			{OutboundTag: "y"},
		}},
		snapshots: []extension.ProbeSnapshot{
			{ProbeTag: "health", OutboundTag: "x"},
			{ProbeTag: "health", OutboundTag: "y"},
		},
	}
	response, err := (&service{observatory: provider}).ListProbeStatuses(context.Background(), &ListProbeStatusesRequest{
		ObserverTag: "health",
		OutboundTag: "y",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(response.Observers) != 1 || len(response.Observers[0].Status) != 1 || response.Observers[0].Status[0].OutboundTag != "y" {
		t.Fatalf("observer status was not filtered by (observer, outbound): %#v", response.Observers)
	}
}

func TestObservatoryResponseSchemasKeepDimensionsSeparate(t *testing.T) {
	fields := (&GetOutboundStatusResponse{}).ProtoReflect().Descriptor().Fields()
	statusField := fields.ByName("status")
	if statusField == nil || statusField.Number() != 1 || statusField.Message().FullName() != "xray.core.app.observatory.ObservationResult" {
		t.Fatalf("legacy status field is not wire-compatible: %v", statusField)
	}
	if fields.Len() != 1 {
		t.Fatalf("GetOutboundStatusResponse fields = %d, want 1", fields.Len())
	}
	probeFields := (&ListProbeStatusesResponse{}).ProtoReflect().Descriptor().Fields()
	observersField := probeFields.ByName("observers")
	if observersField == nil || observersField.Number() != 1 || observersField.Message().FullName() != "xray.core.app.observatory.ObserverStatus" {
		t.Fatalf("probe observers field is invalid: %v", observersField)
	}
	if probeFields.Len() != 1 {
		t.Fatalf("ListProbeStatusesResponse fields = %d, want 1", probeFields.Len())
	}
}

func TestListProbeStatusesReturnsNotFoundForUnknownObserver(t *testing.T) {
	provider := &snapshotObservatory{tags: map[string]struct{}{"known": {}}}
	_, err := (&service{observatory: provider}).ListProbeStatuses(context.Background(), &ListProbeStatusesRequest{ObserverTag: "missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("status code = %v, want NotFound", status.Code(err))
	}
}

var (
	_ extension.Observatory           = (*snapshotObservatory)(nil)
	_ extension.ProbeSnapshotProvider = (*snapshotObservatory)(nil)
	_ features.TaggedFeatures         = (*snapshotObservatory)(nil)
)
