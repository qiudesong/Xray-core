package geodata

import (
	"context"
	stdnet "net"
	"os"
	"sync"

	"github.com/oschwald/geoip2-golang"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/platform/filesystem"
)

const (
	CountryMMDB = "GeoLite2-Country.mmdb"
	ASNMMDB     = "GeoLite2-ASN.mmdb"
	CityMMDB    = "GeoLite2-City.mmdb"
)

type countryMMDB interface {
	Country(stdnet.IP) (*geoip2.Country, error)
	Close() error
}

type asnMMDB interface {
	ASN(stdnet.IP) (*geoip2.ASN, error)
	Close() error
}

type cityMMDB interface {
	City(stdnet.IP) (*geoip2.City, error)
	Close() error
}

type MMDBLookup struct {
	Country        *geoip2.Country
	CountryError   error
	ASN            *geoip2.ASN
	ASNError       error
	City           *geoip2.City
	CityError      error
	CountryEnabled bool
	ASNEnabled     bool
	CityEnabled    bool
}

type MMDBStatus struct {
	Country bool
	ASN     bool
	City    bool
}

type MMDBRegistry struct {
	reloadMu sync.Mutex
	mu       sync.RWMutex
	country  countryMMDB
	asn      asnMMDB
	city     cityMMDB

	openCountry func() (countryMMDB, error)
	openASN     func() (asnMMDB, error)
	openCity    func() (cityMMDB, error)
}

func newMMDBRegistry() *MMDBRegistry {
	return &MMDBRegistry{
		openCountry: func() (countryMMDB, error) {
			reader, err := openMMDB(CountryMMDB)
			if reader == nil {
				return nil, err
			}
			return reader, err
		},
		openASN: func() (asnMMDB, error) {
			reader, err := openMMDB(ASNMMDB)
			if reader == nil {
				return nil, err
			}
			return reader, err
		},
		openCity: func() (cityMMDB, error) {
			reader, err := openMMDB(CityMMDB)
			if reader == nil {
				return nil, err
			}
			return reader, err
		},
	}
}

var MMDBReg = newMMDBRegistry()

func IsMMDBAsset(file string) bool {
	switch file {
	case CountryMMDB, ASNMMDB, CityMMDB:
		return true
	default:
		return false
	}
}

func (r *MMDBRegistry) ReloadAll() error {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()
	return errors.Combine(
		r.reloadCountry(),
		r.reloadASN(),
		r.reloadCity(),
	)
}

func (r *MMDBRegistry) ReloadAsset(file string) error {
	r.reloadMu.Lock()
	defer r.reloadMu.Unlock()

	switch file {
	case CountryMMDB:
		return r.reloadCountry()
	case ASNMMDB:
		return r.reloadASN()
	case CityMMDB:
		return r.reloadCity()
	default:
		return errors.New("unsupported MMDB asset: ", file)
	}
}

func (r *MMDBRegistry) reloadCountry() error {
	database, err := r.openCountry()
	if err != nil {
		return errors.New("failed to open ", CountryMMDB).Base(err)
	}
	r.mu.Lock()
	old := r.country
	r.country = database
	r.mu.Unlock()
	r.closeReplaced(CountryMMDB, old)
	return nil
}

func (r *MMDBRegistry) reloadASN() error {
	database, err := r.openASN()
	if err != nil {
		return errors.New("failed to open ", ASNMMDB).Base(err)
	}
	r.mu.Lock()
	old := r.asn
	r.asn = database
	r.mu.Unlock()
	r.closeReplaced(ASNMMDB, old)
	return nil
}

func (r *MMDBRegistry) reloadCity() error {
	database, err := r.openCity()
	if err != nil {
		return errors.New("failed to open ", CityMMDB).Base(err)
	}
	r.mu.Lock()
	old := r.city
	r.city = database
	r.mu.Unlock()
	r.closeReplaced(CityMMDB, old)
	return nil
}

func (r *MMDBRegistry) closeReplaced(file string, database interface{ Close() error }) {
	if database == nil {
		return
	}
	if err := database.Close(); err != nil {
		errors.LogErrorInner(context.Background(), err, "failed to close replaced MMDB asset ", file)
	}
}

func (r *MMDBRegistry) Lookup(ip stdnet.IP) MMDBLookup {
	r.mu.RLock()
	defer r.mu.RUnlock()

	lookup := MMDBLookup{
		CountryEnabled: r.country != nil,
		ASNEnabled:     r.asn != nil,
		CityEnabled:    r.city != nil,
	}
	if r.country != nil {
		lookup.Country, lookup.CountryError = r.country.Country(ip)
	}
	if r.asn != nil {
		lookup.ASN, lookup.ASNError = r.asn.ASN(ip)
	}
	if r.city != nil {
		lookup.City, lookup.CityError = r.city.City(ip)
	}
	return lookup
}

func (r *MMDBRegistry) Status() MMDBStatus {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return MMDBStatus{
		Country: r.country != nil,
		ASN:     r.asn != nil,
		City:    r.city != nil,
	}
}

func openMMDB(file string) (*geoip2.Reader, error) {
	path, err := filesystem.ResolveAsset(file)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	reader, err := geoip2.Open(path)
	if err != nil {
		if reader == nil {
			return nil, err
		}
		return nil, errors.Combine(err, reader.Close())
	}
	return reader, nil
}
