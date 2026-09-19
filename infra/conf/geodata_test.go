package conf_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xtls/xray-core/app/geodata"
	. "github.com/xtls/xray-core/infra/conf"
)

func TestGeodataConfig(t *testing.T) {
	t.Setenv("xray.location.asset", filepath.Join("..", "..", "resources"))

	creator := func() Buildable {
		return new(GeodataConfig)
	}

	runMultiTestCase(t, []TestCase{
		{
			Input: `{
				"cron": "0 4 * * *",
				"outbound": "proxy",
				"assets": [
					{"url": "https://example.com/geoip.dat", "file": "geoip.dat"},
					{"url": "https://example.com/geosite.dat", "file": "geosite.dat"}
				]
			}`,
			Parser: loadJSON(creator),
			Output: &geodata.Config{
				Cron:     "0 4 * * *",
				Outbound: "proxy",
				Assets: []*geodata.Asset{
					{Url: "https://example.com/geoip.dat", File: "geoip.dat"},
					{Url: "https://example.com/geosite.dat", File: "geosite.dat"},
				},
			},
		},
	})
}

func TestGeodataAssetConfig(t *testing.T) {
	t.Setenv("xray.location.asset", filepath.Join("..", "..", "resources"))

	if _, err := (&GeodataAssetConfig{
		URL:  "https://example.com/geoip.dat",
		File: "geoip.dat",
	}).Build(); err != nil {
		t.Fatal(err)
	}

	if _, err := (&GeodataAssetConfig{
		URL:  "https://example.com/geoip.dat",
		File: "missing.dat",
	}).Build(); err == nil {
		t.Fatal("expected error")
	}
}

func TestGeodataAssetConfigInvalidURL(t *testing.T) {
	t.Setenv("xray.location.asset", filepath.Join("..", "..", "resources"))

	for _, rawURL := range []string{
		"",
		"http://example.com/geoip.dat",
		"ftp://example.com/geoip.dat",
		"https:///geoip.dat",
	} {
		if _, err := (&GeodataAssetConfig{
			URL:  rawURL,
			File: "geoip.dat",
		}).Build(); err == nil {
			t.Fatalf("expected error for %q", rawURL)
		}
	}
}

func TestGeodataConfigSupportsMultipleMMDBAssets(t *testing.T) {
	assetDirectory := t.TempDir()
	t.Setenv("xray.location.asset", assetDirectory)
	files := []string{"GeoLite2-Country.mmdb", "GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb"}
	assets := make([]*GeodataAssetConfig, 0, len(files))
	for _, file := range files {
		if err := os.WriteFile(filepath.Join(assetDirectory, file), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		assets = append(assets, &GeodataAssetConfig{URL: "https://example.com/" + file, File: file})
	}
	cron := "0 4 * * *"
	built, err := (&GeodataConfig{Cron: &cron, Assets: assets}).Build()
	if err != nil {
		t.Fatal(err)
	}
	config := built.(*geodata.Config)
	if len(config.GetAssets()) != len(files) {
		t.Fatalf("MMDB asset count = %d, want %d", len(config.GetAssets()), len(files))
	}
	for i, file := range files {
		if got := config.GetAssets()[i].GetFile(); got != file {
			t.Fatalf("MMDB asset %d file = %q, want %q", i, got, file)
		}
	}
}
