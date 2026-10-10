package index

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
)

// Client-level retries of a bulk or bootstrap request on gateway errors.
// Every write is an idempotent create, so a retried request cannot
// duplicate anything; the indexer's own retry loop handles the rest.
const (
	esMaxRetries   = 3
	esRetryBackoff = 250 * time.Millisecond
)

// NewElasticsearchClient builds the client the indexer uses: addresses,
// basic auth or an API key (the key wins when both are set), TLS and request
// compression from cfg. Request bodies are retried on 502, 503 and 504.
// Other Shadowtap commands that read the indexed documents can share it.
func NewElasticsearchClient(cfg ESConfig) (*elasticsearch.Client, error) {
	es, _, err := newElasticsearchClient(cfg)
	return es, err
}

// newElasticsearchClient also returns the transport, so the indexer can
// release idle connections when it stops.
func newElasticsearchClient(cfg ESConfig) (*elasticsearch.Client, *http.Transport, error) {
	if len(cfg.URLs) == 0 {
		return nil, nil, errors.New("index: elasticsearch.urls is empty")
	}
	addrs := make([]string, len(cfg.URLs))
	for i, u := range cfg.URLs {
		addrs[i] = strings.TrimSpace(u)
	}
	tlsConfig, err := newESTLSConfig(cfg.TLS)
	if err != nil {
		return nil, nil, err
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:       tlsConfig,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	c := elasticsearch.Config{
		Addresses:           addrs,
		Transport:           transport,
		CompressRequestBody: cfg.Compress,
		RetryOnStatus:       []int{http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout},
		MaxRetries:          esMaxRetries,
		RetryBackoff:        func(attempt int) time.Duration { return time.Duration(attempt) * esRetryBackoff },
	}
	switch {
	case cfg.APIKey != "":
		c.APIKey = cfg.APIKey
	case cfg.Username != "":
		c.Username, c.Password = cfg.Username, cfg.Password
	}
	// The Config constructor is deprecated in favour of New(Option...), but
	// a custom transport (the TLS settings above) can only reach New through
	// the elastic-transport module's options, which this module does not
	// depend on directly. The legacy constructor remains fully supported.
	//lint:ignore SA1019 see above
	es, err := elasticsearch.NewClient(c)
	if err != nil {
		return nil, nil, fmt.Errorf("index: create elasticsearch client: %w", err)
	}
	return es, transport, nil
}

// newESTLSConfig builds the TLS settings for https URLs. Without a CA file
// the system roots are used.
func newESTLSConfig(c ESTLS) (*tls.Config, error) {
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.InsecureSkipVerify, //nolint:gosec // explicit opt-in for test clusters
	}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("index: read elasticsearch.tls.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("index: elasticsearch.tls.ca_file %s: no PEM certificates found", c.CAFile)
		}
		tc.RootCAs = pool
	}
	if c.CertFile != "" || c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("index: load elasticsearch client certificate: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}
