package command

import (
	"context"
	"sort"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common"
	core "github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features"
	"github.com/xtls/xray-core/features/extension"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	observerNotFoundMessage          = "observer not found"
	probeSnapshotsUnavailableMessage = "observatory does not expose probe snapshots"
)

type service struct {
	UnimplementedObservatoryServiceServer
	v *core.Instance

	observatory extension.Observatory
}

func (s *service) GetOutboundStatus(ctx context.Context, request *GetOutboundStatusRequest) (*GetOutboundStatusResponse, error) {
	resp, err := s.observatory.GetObservation(ctx)
	if err != nil {
		return nil, err
	}
	retdata := resp.(*observatory.ObservationResult)
	return &GetOutboundStatusResponse{
		Status: retdata,
	}, nil
}

func (s *service) ListProbeStatuses(ctx context.Context, request *ListProbeStatusesRequest) (*ListProbeStatusesResponse, error) {
	if request.ObserverTag != "" {
		container, ok := s.observatory.(features.TaggedFeatures)
		if !ok {
			return nil, status.Error(codes.NotFound, observerNotFoundMessage)
		}
		if _, err := container.GetFeaturesByTag(request.ObserverTag); err != nil {
			return nil, status.Error(codes.NotFound, observerNotFoundMessage)
		}
	}
	provider, ok := s.observatory.(extension.ProbeSnapshotProvider)
	if !ok {
		return nil, status.Error(codes.FailedPrecondition, probeSnapshotsUnavailableMessage)
	}
	snapshots, err := provider.GetProbeSnapshots(ctx, extension.ProbeSnapshotFilter{
		ProbeTag: request.ObserverTag, OutboundTag: request.OutboundTag,
	})
	if err != nil {
		return nil, err
	}
	return s.groupedResponse(snapshots), nil
}

func (s *service) groupedResponse(snapshots []extension.ProbeSnapshot) *ListProbeStatusesResponse {
	groups := make(map[string]*observatory.ObserverStatus)
	for _, snapshot := range snapshots {
		group := groups[snapshot.ProbeTag]
		if group == nil {
			group = &observatory.ObserverStatus{ObserverTag: snapshot.ProbeTag}
			groups[snapshot.ProbeTag] = group
		}
		probeStatus := &observatory.ProbeStatus{
			ObserverTag: snapshot.ProbeTag, OutboundTag: snapshot.OutboundTag, Method: snapshot.Method,
			PolicyState: string(snapshot.PolicyState), EffectiveState: string(snapshot.EffectiveState),
			PolicyReason: snapshot.PolicyReason, Success: snapshot.Latest.Success,
			DurationMs: snapshot.Latest.Duration.Milliseconds(), TtfbMs: snapshot.Latest.TTFB.Milliseconds(),
			HttpStatus: int32(snapshot.Latest.HTTPStatus), Bytes: snapshot.Latest.Bytes,
			ErrorStage: snapshot.Latest.Error.Stage, ErrorReason: snapshot.Latest.Error.Reason,
			WindowSamples: snapshot.WindowSamples, WindowFailures: snapshot.WindowFailures,
			LocationSource: snapshot.LastLocation.Source, Country: snapshot.LastLocation.Country,
		}
		if !snapshot.Latest.CheckedAt.IsZero() {
			probeStatus.CheckedAt = snapshot.Latest.CheckedAt.Unix()
		}
		if !snapshot.LastSuccess.IsZero() {
			probeStatus.LastSuccess = snapshot.LastSuccess.Unix()
		}
		if !snapshot.LastLocation.ObservedAt.IsZero() {
			probeStatus.LocationObservedAt = snapshot.LastLocation.ObservedAt.Unix()
		}
		group.Status = append(group.Status, probeStatus)
	}
	tags := make([]string, 0, len(groups))
	for tag := range groups {
		tags = append(tags, tag)
	}
	sort.Strings(tags)
	response := &ListProbeStatusesResponse{}
	for _, tag := range tags {
		sort.Slice(groups[tag].Status, func(i, j int) bool {
			return groups[tag].Status[i].OutboundTag < groups[tag].Status[j].OutboundTag
		})
		response.Observers = append(response.Observers, groups[tag])
	}
	return response
}

func (s *service) Register(server *grpc.Server) {
	RegisterObservatoryServiceServer(server, s)
}

func init() {
	common.Must(common.RegisterConfig((*Config)(nil), func(ctx context.Context, cfg interface{}) (interface{}, error) {
		s := core.MustFromContext(ctx)
		sv := &service{v: s}
		err := s.RequireFeatures(func(Observatory extension.Observatory) {
			sv.observatory = Observatory
		}, false)
		if err != nil {
			return nil, err
		}
		return sv, nil
	}))
}
