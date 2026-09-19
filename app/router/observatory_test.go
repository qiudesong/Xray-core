package router

import (
	"context"
	"testing"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/features"
	"github.com/xtls/xray-core/features/extension"
	"google.golang.org/protobuf/proto"
)

type routerTestObservatory struct {
	result *observatory.ObservationResult
}

func (*routerTestObservatory) Type() interface{} { return extension.ObservatoryType() }
func (*routerTestObservatory) Start() error      { return nil }
func (*routerTestObservatory) Close() error      { return nil }
func (o *routerTestObservatory) GetObservation(context.Context) (proto.Message, error) {
	return o.result, nil
}

type routerTestTaggedObservatory struct {
	*routerTestObservatory
	members map[string]features.Feature
}

func (o *routerTestTaggedObservatory) GetFeaturesByTag(tag string) (features.Feature, error) {
	feature, found := o.members[tag]
	if !found {
		return nil, errors.New("not found")
	}
	return feature, nil
}

func (o *routerTestTaggedObservatory) GetFeaturesTag() []string {
	result := make([]string, 0, len(o.members))
	for tag := range o.members {
		result = append(result, tag)
	}
	return result
}

func TestObservatoryByTagSelectsMemberAndRejectsUnknown(t *testing.T) {
	member := &routerTestObservatory{result: &observatory.ObservationResult{}}
	container := &routerTestTaggedObservatory{
		routerTestObservatory: &routerTestObservatory{result: &observatory.ObservationResult{}},
		members:               map[string]features.Feature{"health": member},
	}
	selected, err := observatoryByTag(container, "health")
	if err != nil || selected != member {
		t.Fatalf("selected = %#v, err = %v", selected, err)
	}
	if _, err := observatoryByTag(container, "missing"); err == nil {
		t.Fatal("unknown observer tag was accepted")
	}
}

func TestTaggedRoundRobinTreatsMissingProbeResultAsUnavailable(t *testing.T) {
	strategy := &RoundRobinStrategy{
		observerTag: "health",
		observatory: &routerTestObservatory{result: &observatory.ObservationResult{}},
	}
	if selected := strategy.PickOutbound([]string{"unprobed"}); selected != "" {
		t.Fatalf("selected unprobed outbound %q", selected)
	}
}

var (
	_ extension.Observatory   = (*routerTestObservatory)(nil)
	_ features.TaggedFeatures = (*routerTestTaggedObservatory)(nil)
)
