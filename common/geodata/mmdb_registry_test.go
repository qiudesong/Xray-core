package geodata

import (
	"errors"
	stdnet "net"
	"testing"
	"time"

	"github.com/oschwald/geoip2-golang"
	commonerrors "github.com/xtls/xray-core/common/errors"
)

func TestMMDBRegistryReloadsAssetsIndependently(t *testing.T) {
	oldCountry := &fakeCountryMMDB{country: "US"}
	oldASN := &fakeASNMMDB{asn: 64500}
	oldCity := &fakeCityMMDB{city: "Old City"}
	newASN := &fakeASNMMDB{asn: 64501}
	registry := &MMDBRegistry{
		country: oldCountry,
		asn:     oldASN,
		city:    oldCity,
		openASN: func() (asnMMDB, error) { return newASN, nil },
	}

	if err := registry.ReloadAsset(ASNMMDB); err != nil {
		t.Fatal(err)
	}
	if !oldASN.closed {
		t.Fatal("replaced ASN database was not closed")
	}
	if oldCountry.closed || oldCity.closed {
		t.Fatal("reloading ASN closed an unrelated database")
	}
	lookup := registry.Lookup(stdnet.ParseIP("203.0.113.10"))
	if lookup.ASN == nil || lookup.ASN.AutonomousSystemNumber != 64501 {
		t.Fatalf("unexpected ASN lookup: %+v", lookup.ASN)
	}
	if lookup.Country == nil || lookup.Country.Country.IsoCode != "US" {
		t.Fatalf("unexpected Country lookup: %+v", lookup.Country)
	}
	if lookup.City == nil || lookup.City.City.Names["en"] != "Old City" {
		t.Fatalf("unexpected City lookup: %+v", lookup.City)
	}
}

func TestMMDBRegistryReloadAllKeepsFailedAsset(t *testing.T) {
	oldCountry := &fakeCountryMMDB{country: "US"}
	oldASN := &fakeASNMMDB{asn: 64500}
	oldCity := &fakeCityMMDB{city: "Old City"}
	newCountry := &fakeCountryMMDB{country: "CN"}
	newCity := &fakeCityMMDB{city: "New City"}
	want := errors.New("invalid ASN database")
	registry := &MMDBRegistry{
		country: oldCountry,
		asn:     oldASN,
		city:    oldCity,
		openCountry: func() (countryMMDB, error) {
			return newCountry, nil
		},
		openASN: func() (asnMMDB, error) {
			return nil, want
		},
		openCity: func() (cityMMDB, error) {
			return newCity, nil
		},
	}

	if err := registry.ReloadAll(); err == nil || !commonerrors.AllEqual(want, err) {
		t.Fatalf("reload error = %v, want %v", err, want)
	}
	if !oldCountry.closed || !oldCity.closed {
		t.Fatal("successfully replaced databases were not closed")
	}
	if oldASN.closed {
		t.Fatal("failed ASN reload replaced the existing database")
	}
	lookup := registry.Lookup(stdnet.ParseIP("203.0.113.10"))
	if lookup.Country == nil || lookup.Country.Country.IsoCode != "CN" {
		t.Fatalf("unexpected Country lookup: %+v", lookup.Country)
	}
	if lookup.ASN == nil || lookup.ASN.AutonomousSystemNumber != 64500 {
		t.Fatalf("unexpected ASN lookup: %+v", lookup.ASN)
	}
	if lookup.City == nil || lookup.City.City.Names["en"] != "New City" {
		t.Fatalf("unexpected City lookup: %+v", lookup.City)
	}
}

func TestMMDBRegistryWaitsForLookupBeforeClosingReplacedDatabase(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	oldCountry := &fakeCountryMMDB{country: "US", entered: entered, release: release}
	newCountry := &fakeCountryMMDB{country: "CN"}
	registry := &MMDBRegistry{
		country: oldCountry,
		openCountry: func() (countryMMDB, error) {
			return newCountry, nil
		},
	}

	lookupDone := make(chan struct{})
	go func() {
		registry.Lookup(stdnet.ParseIP("203.0.113.10"))
		close(lookupDone)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("lookup did not start")
	}

	reloadDone := make(chan error, 1)
	go func() {
		reloadDone <- registry.ReloadAsset(CountryMMDB)
	}()
	select {
	case err := <-reloadDone:
		t.Fatalf("reload completed while old database was in use: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if oldCountry.closed {
		t.Fatal("old database was closed while a lookup was in progress")
	}

	close(release)
	select {
	case <-lookupDone:
	case <-time.After(time.Second):
		t.Fatal("lookup did not finish")
	}
	select {
	case err := <-reloadDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("reload did not finish")
	}
	if !oldCountry.closed {
		t.Fatal("old database was not closed after lookup finished")
	}
}

type fakeCountryMMDB struct {
	country string
	entered chan struct{}
	release chan struct{}
	closed  bool
}

func (d *fakeCountryMMDB) Country(stdnet.IP) (*geoip2.Country, error) {
	if d.entered != nil {
		close(d.entered)
		<-d.release
	}
	record := new(geoip2.Country)
	record.Country.IsoCode = d.country
	return record, nil
}

func (d *fakeCountryMMDB) Close() error {
	d.closed = true
	return nil
}

type fakeASNMMDB struct {
	asn    uint
	closed bool
}

func (d *fakeASNMMDB) ASN(stdnet.IP) (*geoip2.ASN, error) {
	return &geoip2.ASN{AutonomousSystemNumber: d.asn}, nil
}

func (d *fakeASNMMDB) Close() error {
	d.closed = true
	return nil
}

type fakeCityMMDB struct {
	city   string
	closed bool
}

func (d *fakeCityMMDB) City(stdnet.IP) (*geoip2.City, error) {
	record := new(geoip2.City)
	record.City.Names = map[string]string{"en": d.city}
	return record, nil
}

func (d *fakeCityMMDB) Close() error {
	d.closed = true
	return nil
}
