package geodata

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	commongeodata "github.com/xtls/xray-core/common/geodata"
)

func TestReloadWithUpdateReloadsRestoredAssetsAfterRollback(t *testing.T) {
	assetDirectory := t.TempDir()
	t.Setenv("xray.location.asset", assetDirectory)
	assetFile := "test.mmdb"
	assetPath := filepath.Join(assetDirectory, assetFile)
	if err := os.WriteFile(assetPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	g := &Instance{
		assets: []*Asset{{Url: "http://example.com/test.mmdb", File: assetFile}},
		downloader: &downloader{
			ctx: context.Background(),
			httpClient: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("new")),
					Header:     make(http.Header),
				}, nil
			})},
		},
	}

	want := errors.New("reject updated asset")
	calls := 0
	g.reloadData = func() error {
		calls++
		if calls == 1 {
			return want
		}
		return nil
	}

	if err := g.reloadWithUpdate(); err == nil || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("reload error = %v, want %v", err, want)
	}
	if calls != 2 {
		t.Fatalf("geodata reload calls = %d, want 2", calls)
	}
	content, err := os.ReadFile(assetPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(content); got != "old" {
		t.Fatalf("asset content after rollback = %q, want old", got)
	}
}

func TestReloadWithUpdateUpdatesMMDBAssetsIndependently(t *testing.T) {
	assetDirectory := t.TempDir()
	t.Setenv("xray.location.asset", assetDirectory)
	files := []string{commongeodata.CountryMMDB, commongeodata.ASNMMDB, commongeodata.CityMMDB}
	assets := make([]*Asset, 0, len(files))
	for _, file := range files {
		if err := os.WriteFile(filepath.Join(assetDirectory, file), []byte("old-"+file), 0o600); err != nil {
			t.Fatal(err)
		}
		assets = append(assets, &Asset{Url: "http://example.com/" + file, File: file})
	}

	want := errors.New("reject City database")
	var reloaded []string
	g := &Instance{
		assets: assets,
		downloader: &downloader{
			ctx: context.Background(),
			httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				file := filepath.Base(request.URL.Path)
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(strings.NewReader("new-" + file)),
					Header:     make(http.Header),
				}, nil
			})},
		},
		reloadMMDB: func(file string) error {
			reloaded = append(reloaded, file)
			if file == commongeodata.CityMMDB {
				return want
			}
			return nil
		},
	}

	if err := g.reloadWithUpdate(); err == nil || !strings.Contains(err.Error(), want.Error()) {
		t.Fatalf("reload error = %v, want %v", err, want)
	}
	if len(reloaded) != len(files) {
		t.Fatalf("reloaded MMDB assets = %v, want %v", reloaded, files)
	}
	for _, file := range files {
		content, err := os.ReadFile(filepath.Join(assetDirectory, file))
		if err != nil {
			t.Fatal(err)
		}
		prefix := "new-"
		if file == commongeodata.CityMMDB {
			prefix = "old-"
		}
		if got, expected := string(content), prefix+file; got != expected {
			t.Fatalf("%s content = %q, want %q", file, got, expected)
		}
	}
}

func TestReloadWithUpdateContinuesAfterMMDBDownloadFailure(t *testing.T) {
	assetDirectory := t.TempDir()
	t.Setenv("xray.location.asset", assetDirectory)
	files := []string{commongeodata.CountryMMDB, commongeodata.ASNMMDB, commongeodata.CityMMDB}
	assets := make([]*Asset, 0, len(files))
	for _, file := range files {
		if err := os.WriteFile(filepath.Join(assetDirectory, file), []byte("old-"+file), 0o600); err != nil {
			t.Fatal(err)
		}
		assets = append(assets, &Asset{Url: "http://example.com/" + file, File: file})
	}

	var reloaded []string
	g := &Instance{
		assets: assets,
		downloader: &downloader{
			ctx: context.Background(),
			httpClient: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				file := filepath.Base(request.URL.Path)
				status := http.StatusOK
				if file == commongeodata.CountryMMDB {
					status = http.StatusInternalServerError
				}
				return &http.Response{
					StatusCode: status,
					Body:       io.NopCloser(strings.NewReader("new-" + file)),
					Header:     make(http.Header),
				}, nil
			})},
		},
		reloadMMDB: func(file string) error {
			reloaded = append(reloaded, file)
			return nil
		},
	}

	if err := g.reloadWithUpdate(); err == nil || !strings.Contains(err.Error(), "unexpected status code") {
		t.Fatalf("reload error = %v, want download failure", err)
	}
	wantReloaded := []string{commongeodata.ASNMMDB, commongeodata.CityMMDB}
	if strings.Join(reloaded, ",") != strings.Join(wantReloaded, ",") {
		t.Fatalf("reloaded MMDB assets = %v, want %v", reloaded, wantReloaded)
	}
	for _, file := range files {
		content, err := os.ReadFile(filepath.Join(assetDirectory, file))
		if err != nil {
			t.Fatal(err)
		}
		prefix := "new-"
		if file == commongeodata.CountryMMDB {
			prefix = "old-"
		}
		if got, expected := string(content), prefix+file; got != expected {
			t.Fatalf("%s content = %q, want %q", file, got, expected)
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}
