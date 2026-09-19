package router

import (
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/features"
	"github.com/xtls/xray-core/features/extension"
)

func observatoryByTag(base extension.Observatory, tag string) (extension.Observatory, error) {
	if tag == "" {
		return base, nil
	}
	container, ok := base.(features.TaggedFeatures)
	if !ok {
		return nil, errors.New("configured observerTag requires a tagged observatory: ", tag)
	}
	feature, err := container.GetFeaturesByTag(tag)
	if err != nil {
		return nil, err
	}
	selected, ok := feature.(extension.Observatory)
	if !ok {
		return nil, errors.New("tagged feature is not an observatory: ", tag)
	}
	return selected, nil
}
