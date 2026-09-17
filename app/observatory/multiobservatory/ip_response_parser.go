package multiobservatory

import (
	"encoding/json"
	"fmt"
	"net/netip"
	"net/url"
	"strings"
)

const (
	locationSourceCloudflare = "cloudflare"
	locationSourceCountryIs  = "country.is"
)

type ipObservation struct {
	address netip.Addr
	country string
	source  string
}

type ipResponseParser interface {
	Type() string
	Parse(body []byte) (ipObservation, error)
}

type cloudflareTraceParser struct{}

func (cloudflareTraceParser) Type() string { return IPProbeProviderTypeCloudflareTrace }

func (cloudflareTraceParser) Parse(body []byte) (ipObservation, error) {
	values := make(map[string]string)
	for _, line := range strings.Split(string(body), "\n") {
		key, value, found := strings.Cut(strings.TrimSpace(line), "=")
		if found {
			values[strings.TrimSpace(key)] = strings.TrimSpace(value)
		}
	}
	return newIPObservation(values["ip"], values["loc"], locationSourceCloudflare)
}

type countryIsParser struct{}

func (countryIsParser) Type() string { return IPProbeProviderTypeCountryIs }

func (countryIsParser) Parse(body []byte) (ipObservation, error) {
	var response struct {
		IP          string `json:"ip"`
		Country     string `json:"country"`
		CountryCode string `json:"country_code"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return ipObservation{}, fmt.Errorf("invalid country.is response: %w", err)
	}
	country := response.Country
	// Country.is returns its ISO code in country. Accept country_code as a
	// defensive fallback for compatible deployments.
	if country == "" {
		country = response.CountryCode
	}
	return newIPObservation(response.IP, country, locationSourceCountryIs)
}

func newIPObservation(rawIP, rawCountry, source string) (ipObservation, error) {
	address, err := netip.ParseAddr(strings.TrimSpace(rawIP))
	if err != nil {
		return ipObservation{}, fmt.Errorf("invalid IP address: %w", err)
	}
	country := strings.ToUpper(strings.TrimSpace(rawCountry))
	if !validCountryCode(country) {
		return ipObservation{}, fmt.Errorf("invalid country code")
	}
	return ipObservation{address: address.Unmap(), country: country, source: source}, nil
}

func validCountryCode(country string) bool {
	return len(country) == 2 && country[0] >= 'A' && country[0] <= 'Z' && country[1] >= 'A' && country[1] <= 'Z'
}

func buildIPResponseParser(providerType string) (ipResponseParser, error) {
	switch strings.TrimSpace(providerType) {
	case IPProbeProviderTypeCloudflareTrace:
		return cloudflareTraceParser{}, nil
	case IPProbeProviderTypeCountryIs:
		return countryIsParser{}, nil
	default:
		return nil, fmt.Errorf("unknown IP probe provider type: %s", providerType)
	}
}

// ResolveIPProbeProvider applies a built-in provider preset and validates an
// optional compatible endpoint override. The returned config is detached from
// the input so callers can safely retain their original configuration.
func ResolveIPProbeProvider(config *IPProbeProviderConfig) (*IPProbeProviderConfig, error) {
	resolved := &IPProbeProviderConfig{}
	if config != nil {
		resolved.Type = strings.TrimSpace(config.Type)
		resolved.Url = strings.TrimSpace(config.Url)
	}
	if resolved.Type == "" {
		if resolved.Url != "" {
			return nil, fmt.Errorf("IP probe provider type is required for a custom URL")
		}
		resolved.Type = IPProbeProviderTypeCloudflareTrace
	}
	switch resolved.Type {
	case IPProbeProviderTypeCloudflareTrace:
		if resolved.Url == "" {
			resolved.Url = DefaultCloudflareTraceURL
		}
	case IPProbeProviderTypeCountryIs:
		if resolved.Url == "" {
			resolved.Url = DefaultCountryIsURL
		}
	default:
		return nil, fmt.Errorf("unknown IP probe provider type: %s", resolved.Type)
	}
	parsedURL, err := url.Parse(resolved.Url)
	if err != nil || parsedURL.Host == "" || (parsedURL.Scheme != URLSchemeHTTP && parsedURL.Scheme != URLSchemeHTTPS) {
		return nil, fmt.Errorf("IP probe provider URL must be an absolute HTTP or HTTPS URL")
	}
	return resolved, nil
}
