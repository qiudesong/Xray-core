package metrics

import (
	"hash/maphash"
	stdnet "net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oschwald/geoip2-golang"
	"github.com/xtls/xray-core/common/errors"
	commongeodata "github.com/xtls/xray-core/common/geodata"
	"github.com/xtls/xray-core/common/log"
	xnet "github.com/xtls/xray-core/common/net"
)

type accessRole string

const (
	accessRoleServer accessRole = "server"
	accessRoleClient accessRole = "client"

	defaultAccessMetricsWindow    = 5 * time.Minute
	defaultAccessMetricsQueueSize = 4096
	maxAccessMetricsQueueSize     = 1 << 20
	maxTrackedAccessSeries        = 4096
	maxTrackedAccessGeoSeries     = 4096
	maxTrackedAccessAddressSeries = 4096
	maxTrackedAccessUsers         = 100000
	accessUnknown                 = "unknown"
	accessWindowBucketCount       = 60
	accessSideFrom                = "from"
	accessSideTo                  = "to"
)

type countryLookup interface {
	Country(stdnet.IP) (*geoip2.Country, error)
	Close() error
}

type asnLookup interface {
	ASN(stdnet.IP) (*geoip2.ASN, error)
	Close() error
}

type cityLookup interface {
	City(stdnet.IP) (*geoip2.City, error)
	Close() error
}

type accessGeoProvider interface {
	Lookup(stdnet.IP) commongeodata.MMDBLookup
	Status() commongeodata.MMDBStatus
}

type accessLabels struct {
	inbound  string
	outbound string
	network  string
	status   string
}

type accessPeerCountryLabels struct {
	country string
	side    string
	tag     string
	network string
}

type accessPeerASNLabels struct {
	asn     string
	org     string
	side    string
	tag     string
	network string
}

type accessPeerCityLabels struct {
	country string
	city    string
	side    string
	tag     string
	network string
}

type accessAddressLabels struct {
	address     string
	addressType string
	side        string
	status      string
}

type accessWindowBucket struct {
	slot     int64
	requests map[string]uint64
}

type accessMetrics struct {
	window      time.Duration
	bucketWidth time.Duration
	queueSize   int
	role        accessRole
	geo         accessGeoProvider
	userSeed    maphash.Seed

	mu                     sync.RWMutex
	requests               map[accessLabels]uint64
	peerCountryConnections map[accessPeerCountryLabels]uint64
	peerASNConnections     map[accessPeerASNLabels]uint64
	peerCityConnections    map[accessPeerCityLabels]uint64
	addressRequests        map[accessAddressLabels]uint64
	windowBuckets          []accessWindowBucket
	users                  map[uint64]time.Time
	requestSeriesDropped   uint64
	peerASNSeriesDropped   uint64
	peerCitySeriesDropped  uint64
	addressSeriesDropped   uint64
	userTrackingDropped    uint64

	subscription     *log.AccessSubscription
	peerSubscription *log.PeerSubscription
	stop             chan struct{}
	close            sync.Once
	wg               sync.WaitGroup
}

type accessRequestSample struct {
	labels accessLabels
	value  uint64
}

type accessPeerCountrySample struct {
	labels accessPeerCountryLabels
	value  uint64
}

type accessPeerASNSample struct {
	labels accessPeerASNLabels
	value  uint64
}

type accessPeerCitySample struct {
	labels accessPeerCityLabels
	value  uint64
}

type accessAddressSample struct {
	labels accessAddressLabels
	value  uint64
}

type accessMetricsSnapshot struct {
	requests               []accessRequestSample
	peerCountryConnections []accessPeerCountrySample
	peerASNConnections     []accessPeerASNSample
	peerCityConnections    []accessPeerCitySample
	addressRequests        []accessAddressSample
	windowRequests         map[string]uint64
	uniqueUsers            int
	eventsDropped          uint64
	peerEventsDropped      uint64
	requestSeriesDropped   uint64
	peerASNSeriesDropped   uint64
	peerCitySeriesDropped  uint64
	addressSeriesDropped   uint64
	userTrackingDropped    uint64
	asnEnabled             bool
	cityEnabled            bool
	role                   string
	peerSide               string
}

func newAccessMetrics(config *AccessMetricsConfig) (*accessMetrics, error) {
	if config == nil || !config.GetEnabled() {
		return nil, nil
	}
	metrics := new(accessMetrics)
	if err := metrics.setRole(config.GetRole()); err != nil {
		return nil, err
	}

	window := time.Duration(config.GetWindow())
	if window == 0 {
		window = defaultAccessMetricsWindow
	}
	if window < time.Second {
		return nil, errors.New("access metrics window must be at least one second")
	}

	queueSizeValue := config.GetQueueSize()
	if queueSizeValue > maxAccessMetricsQueueSize {
		return nil, errors.New("access metrics queue size exceeds ", maxAccessMetricsQueueSize)
	}
	queueSize := int(queueSizeValue)
	if queueSizeValue == 0 {
		queueSize = defaultAccessMetricsQueueSize
	}

	bucketWidth := window / accessWindowBucketCount
	if window%accessWindowBucketCount != 0 {
		bucketWidth++
	}
	if bucketWidth < time.Second {
		bucketWidth = time.Second
	}
	bucketCount := int(window / bucketWidth)
	if window%bucketWidth != 0 {
		bucketCount++
	}
	bucketCount++

	metrics.window = window
	metrics.bucketWidth = bucketWidth
	metrics.queueSize = queueSize
	metrics.userSeed = maphash.MakeSeed()
	metrics.requests = make(map[accessLabels]uint64)
	metrics.peerCountryConnections = make(map[accessPeerCountryLabels]uint64)
	metrics.peerASNConnections = make(map[accessPeerASNLabels]uint64)
	metrics.peerCityConnections = make(map[accessPeerCityLabels]uint64)
	metrics.addressRequests = make(map[accessAddressLabels]uint64)
	metrics.windowBuckets = make([]accessWindowBucket, bucketCount)
	metrics.users = make(map[uint64]time.Time)
	metrics.stop = make(chan struct{})
	metrics.geo = commongeodata.MMDBReg

	if err := commongeodata.MMDBReg.ReloadAll(); err != nil {
		return nil, err
	}

	return metrics, nil
}

func (m *accessMetrics) Start() {
	if m == nil || m.subscription != nil {
		return
	}
	m.subscription = log.SubscribeAccessEvents(m.queueSize)
	m.peerSubscription = log.SubscribePeerEvents(m.peerEventSide(), m.queueSize)
	m.wg.Add(1)
	go m.run()
}

func (m *accessMetrics) Close() error {
	if m == nil {
		return nil
	}
	var err error
	m.close.Do(func() {
		if m.subscription != nil {
			close(m.stop)
			m.subscription.Close()
			m.peerSubscription.Close()
			m.wg.Wait()
		}
	})
	return err
}

func (m *accessMetrics) run() {
	defer m.wg.Done()
	cleanupInterval := min(m.window, time.Minute)
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		// Prefer shutdown over a ready buffered event so Close does not drain the backlog.
		select {
		case <-m.stop:
			return
		default:
		}

		select {
		case <-m.stop:
			return
		case event, ok := <-m.subscription.Events():
			if !ok {
				return
			}
			m.record(event)
		case event, ok := <-m.peerSubscription.Events():
			if !ok {
				return
			}
			m.recordPeer(event)
		case now := <-ticker.C:
			m.expireUsers(now)
		}
	}
}

func (m *accessMetrics) record(event log.AccessEvent) {
	message := event.Message
	status := string(message.Status)
	labels := accessLabels{
		inbound:  m.normalizedAccessLabel(message.InboundTag),
		outbound: m.normalizedAccessLabel(message.OutboundTag),
		network:  m.normalizedAccessLabel(message.Network),
		status:   status,
	}

	reportedAddress, addressSide := m.metricAddress(message)

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, found := m.requests[labels]; found || len(m.requests) < maxTrackedAccessSeries {
		m.requests[labels]++
	} else {
		m.requestSeriesDropped++
	}
	m.recordWindowRequest(event.Time, status)
	if reportedAddress.Value != "" {
		labels := accessAddressLabels{
			address:     reportedAddress.Value,
			addressType: string(reportedAddress.Type),
			side:        addressSide,
			status:      status,
		}
		if _, found := m.addressRequests[labels]; found || len(m.addressRequests) < maxTrackedAccessAddressSeries {
			m.addressRequests[labels]++
		} else {
			m.addressSeriesDropped++
		}
	}
	if message.Status == log.AccessAccepted && message.Email != "" {
		m.recordUser(event.Time, message.Email)
	}
}

func (m *accessMetrics) recordPeer(event log.PeerEvent) {
	if event.Side != m.peerEventSide() {
		return
	}
	ip := m.accessAddressIP(event.Address)
	if m.geo == nil || !m.isPublicAccessIP(ip) {
		return
	}

	side := m.normalizedAccessLabel(string(event.Side))
	tag := m.normalizedAccessLabel(event.Tag)
	network := m.normalizedAccessLabel(event.Network)
	lookup := m.geo.Lookup(ip)
	country := ""
	var asn accessPeerASNLabels
	var city accessPeerCityLabels
	if lookup.CountryEnabled {
		country = m.lookupCountry(lookup.Country, lookup.CountryError)
	}
	if lookup.ASNEnabled {
		asn = m.lookupPeerASN(lookup.ASN, lookup.ASNError, side, tag, network)
	}
	if lookup.CityEnabled {
		city = m.lookupPeerCity(lookup.City, lookup.CityError, side, tag, network)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if country != "" {
		labels := accessPeerCountryLabels{country: country, side: side, tag: tag, network: network}
		m.peerCountryConnections[labels]++
	}
	if asn.asn != "" {
		if _, found := m.peerASNConnections[asn]; found || len(m.peerASNConnections) < maxTrackedAccessGeoSeries {
			m.peerASNConnections[asn]++
		} else {
			m.peerASNSeriesDropped++
		}
	}
	if city.city != "" {
		if _, found := m.peerCityConnections[city]; found || len(m.peerCityConnections) < maxTrackedAccessGeoSeries {
			m.peerCityConnections[city]++
		} else {
			m.peerCitySeriesDropped++
		}
	}
}

func (m *accessMetrics) recordWindowRequest(at time.Time, status string) {
	slot := at.UnixNano() / int64(m.bucketWidth)
	bucket := &m.windowBuckets[int(slot%int64(len(m.windowBuckets)))]
	if bucket.slot != slot || bucket.requests == nil {
		bucket.slot = slot
		bucket.requests = make(map[string]uint64, 2)
	}
	bucket.requests[status]++
}

func (m *accessMetrics) recordUser(at time.Time, email string) {
	var hash maphash.Hash
	hash.SetSeed(m.userSeed)
	_, _ = hash.WriteString(email)
	key := hash.Sum64()
	if _, found := m.users[key]; !found && len(m.users) >= maxTrackedAccessUsers {
		m.userTrackingDropped++
		return
	}
	m.users[key] = at
}

func (m *accessMetrics) expireUsers(now time.Time) {
	cutoff := now.Add(-m.window)
	m.mu.Lock()
	defer m.mu.Unlock()
	for user, lastSeen := range m.users {
		if lastSeen.Before(cutoff) {
			delete(m.users, user)
		}
	}
}

func (m *accessMetrics) snapshot(now time.Time) accessMetricsSnapshot {
	var geoStatus commongeodata.MMDBStatus
	if m.geo != nil {
		geoStatus = m.geo.Status()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	snapshot := accessMetricsSnapshot{
		requests:               make([]accessRequestSample, 0, len(m.requests)),
		peerCountryConnections: make([]accessPeerCountrySample, 0, len(m.peerCountryConnections)),
		peerASNConnections:     make([]accessPeerASNSample, 0, len(m.peerASNConnections)),
		peerCityConnections:    make([]accessPeerCitySample, 0, len(m.peerCityConnections)),
		addressRequests:        make([]accessAddressSample, 0, len(m.addressRequests)),
		windowRequests:         make(map[string]uint64),
		requestSeriesDropped:   m.requestSeriesDropped,
		peerASNSeriesDropped:   m.peerASNSeriesDropped,
		peerCitySeriesDropped:  m.peerCitySeriesDropped,
		addressSeriesDropped:   m.addressSeriesDropped,
		userTrackingDropped:    m.userTrackingDropped,
		asnEnabled:             geoStatus.ASN,
		cityEnabled:            geoStatus.City,
		role:                   string(m.role),
		peerSide:               string(m.peerEventSide()),
	}
	if m.subscription != nil {
		snapshot.eventsDropped = m.subscription.Dropped()
	}
	if m.peerSubscription != nil {
		snapshot.peerEventsDropped = m.peerSubscription.Dropped()
	}
	for labels, value := range m.requests {
		snapshot.requests = append(snapshot.requests, accessRequestSample{labels: labels, value: value})
	}
	for labels, value := range m.peerCountryConnections {
		snapshot.peerCountryConnections = append(snapshot.peerCountryConnections, accessPeerCountrySample{labels: labels, value: value})
	}
	for labels, value := range m.peerASNConnections {
		snapshot.peerASNConnections = append(snapshot.peerASNConnections, accessPeerASNSample{labels: labels, value: value})
	}
	for labels, value := range m.peerCityConnections {
		snapshot.peerCityConnections = append(snapshot.peerCityConnections, accessPeerCitySample{labels: labels, value: value})
	}
	for labels, value := range m.addressRequests {
		snapshot.addressRequests = append(snapshot.addressRequests, accessAddressSample{labels: labels, value: value})
	}

	cutoff := now.Add(-m.window)
	oldestSlot := cutoff.UnixNano() / int64(m.bucketWidth)
	for i := range m.windowBuckets {
		bucket := &m.windowBuckets[i]
		if bucket.slot < oldestSlot {
			continue
		}
		for status, value := range bucket.requests {
			snapshot.windowRequests[status] += value
		}
	}
	for user, lastSeen := range m.users {
		if lastSeen.Before(cutoff) {
			delete(m.users, user)
			continue
		}
		snapshot.uniqueUsers++
	}
	return snapshot
}

func (m *accessMetrics) lookupCountry(record *geoip2.Country, err error) string {
	if err != nil || record == nil {
		return accessUnknown
	}
	if country := record.Country.IsoCode; country != "" {
		return country
	}
	if country := record.RegisteredCountry.IsoCode; country != "" {
		return country
	}
	return accessUnknown
}

func (m *accessMetrics) lookupPeerASN(record *geoip2.ASN, err error, side, tag, network string) accessPeerASNLabels {
	labels := accessPeerASNLabels{asn: accessUnknown, org: accessUnknown, side: side, tag: tag, network: network}
	if err != nil || record == nil {
		return labels
	}
	if record.AutonomousSystemNumber != 0 {
		labels.asn = strconv.FormatUint(uint64(record.AutonomousSystemNumber), 10)
	}
	if org := record.AutonomousSystemOrganization; org != "" {
		labels.org = org
	}
	return labels
}

func (m *accessMetrics) lookupPeerCity(record *geoip2.City, err error, side, tag, network string) accessPeerCityLabels {
	labels := accessPeerCityLabels{country: accessUnknown, city: accessUnknown, side: side, tag: tag, network: network}
	if err != nil || record == nil {
		return labels
	}
	if country := record.Country.IsoCode; country != "" {
		labels.country = country
	} else if country := record.RegisteredCountry.IsoCode; country != "" {
		labels.country = country
	}
	if city := record.City.Names["en"]; city != "" {
		labels.city = city
	}
	return labels
}

func (m *accessMetrics) metricAddress(message log.AccessMessage) (reportedAddress log.AccessAddress, side string) {
	destination := m.accessDestination(message)
	if m.role == accessRoleClient {
		return m.accessAddress(message.From), accessSideFrom
	}
	return destination, accessSideTo
}

func (m *accessMetrics) peerEventSide() log.PeerSide {
	if m.role == accessRoleClient {
		return log.PeerSideOutbound
	}
	return log.PeerSideInbound
}

func (m *accessMetrics) accessDestination(message log.AccessMessage) log.AccessAddress {
	if message.Destination.Value != "" {
		return message.Destination
	}
	return m.accessAddress(message.To)
}

func (m *accessMetrics) accessAddress(value interface{}) log.AccessAddress {
	switch source := value.(type) {
	case xnet.Destination:
		if source.Network != xnet.Network_UNIX {
			return m.accessNetworkAddress(source.Address)
		}
	case *xnet.Destination:
		if source != nil && source.Network != xnet.Network_UNIX {
			return m.accessNetworkAddress(source.Address)
		}
	case xnet.Address:
		return m.accessNetworkAddress(source)
	case stdnet.IP:
		return m.accessAddressFromIP(source)
	case *stdnet.TCPAddr:
		if source != nil {
			return m.accessAddressFromIP(source.IP)
		}
	case *stdnet.UDPAddr:
		if source != nil {
			return m.accessAddressFromIP(source.IP)
		}
	case *stdnet.IPAddr:
		if source != nil {
			return m.accessAddressFromIP(source.IP)
		}
	case url.URL:
		return m.accessAddressFromHost(source.Hostname())
	case *url.URL:
		if source != nil {
			return m.accessAddressFromHost(source.Hostname())
		}
	case string:
		if strings.Contains(source, "://") || strings.HasPrefix(source, "//") {
			if parsed, err := url.Parse(source); err == nil {
				return m.accessAddressFromHost(parsed.Hostname())
			}
		}
		if host, _, err := stdnet.SplitHostPort(source); err == nil {
			return m.accessAddressFromHost(host)
		}
		return m.accessAddressFromHost(source)
	case stdnet.Addr:
		host, _, err := stdnet.SplitHostPort(source.String())
		if err == nil {
			return m.accessAddressFromHost(host)
		}
	}
	return log.AccessAddress{}
}

func (m *accessMetrics) accessNetworkAddress(address xnet.Address) log.AccessAddress {
	if address == nil {
		return log.AccessAddress{}
	}
	if address.Family().IsIP() {
		return m.accessAddressFromIP(address.IP())
	}
	if address.Family().IsDomain() {
		domain := address.Domain()
		if domain != "" {
			return log.AccessAddress{Value: domain, Type: log.AccessAddressTypeDomain}
		}
	}
	return log.AccessAddress{}
}

func (m *accessMetrics) accessAddressFromIP(ip stdnet.IP) log.AccessAddress {
	if ip == nil {
		return log.AccessAddress{}
	}
	return log.AccessAddress{Value: ip.String(), Type: log.AccessAddressTypeIP}
}

func (m *accessMetrics) accessAddressFromHost(host string) log.AccessAddress {
	if host == "" {
		return log.AccessAddress{}
	}
	addressType := log.AccessAddressTypeDomain
	if ip := m.parseAccessIPHost(host); ip != nil {
		addressType = log.AccessAddressTypeIP
	}
	return log.AccessAddress{Value: host, Type: addressType}
}

func (m *accessMetrics) accessAddressIP(address log.AccessAddress) stdnet.IP {
	if address.Type != log.AccessAddressTypeIP {
		return nil
	}
	return m.parseAccessIPHost(address.Value)
}

func (m *accessMetrics) parseAccessIPHost(host string) stdnet.IP {
	host = strings.TrimSpace(host)
	if zone := strings.LastIndexByte(host, '%'); zone >= 0 {
		host = host[:zone]
	}
	return stdnet.ParseIP(strings.Trim(host, "[]"))
}

func (m *accessMetrics) isPublicAccessIP(ip stdnet.IP) bool {
	return ip != nil && ip.IsGlobalUnicast() && !ip.IsPrivate()
}

func (m *accessMetrics) setRole(role string) error {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case string(accessRoleServer):
		m.role = accessRoleServer
	case string(accessRoleClient):
		m.role = accessRoleClient
	default:
		return errors.New("access metrics role is required and must be server or client")
	}
	return nil
}

func (m *accessMetrics) normalizedAccessLabel(value string) string {
	if value == "" {
		return accessUnknown
	}
	return value
}
