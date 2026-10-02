package kafka

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
)

// newOptionsClient builds a client from ClientOptions(cfg) so the options can
// be inspected with OptValue. Building a client does not connect.
func newOptionsClient(t *testing.T, cfg Config) *kgo.Client {
	t.Helper()
	opts, err := ClientOptions(cfg)
	if err != nil {
		t.Fatalf("ClientOptions: %v", err)
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatalf("kgo.NewClient: %v", err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func ping(cl *kgo.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return cl.Ping(ctx)
}

func TestClientOptionsBasics(t *testing.T) {
	cl := newOptionsClient(t, Config{
		Brokers:  []string{" 127.0.0.1:9092 ", "kafka-1.kafka:9092"},
		ClientID: "shadowtap-tap-0",
	})
	if got, want := cl.OptValue(kgo.SeedBrokers), []string{"127.0.0.1:9092", "kafka-1.kafka:9092"}; !reflect.DeepEqual(got, want) {
		t.Errorf("SeedBrokers = %v, want %v", got, want)
	}
	if got := cl.OptValue(kgo.ClientID); got != "shadowtap-tap-0" {
		t.Errorf("ClientID = %v, want shadowtap-tap-0", got)
	}
	if got, _ := cl.OptValue(kgo.DialTLSConfig).(*tls.Config); got != nil {
		t.Errorf("DialTLSConfig = %+v, want nil without tls.enabled", got)
	}
	if got, _ := cl.OptValue(kgo.SASL).([]sasl.Mechanism); len(got) != 0 {
		t.Errorf("SASL = %v, want none", got)
	}
}

func TestClientOptionsRejectsBadConnectionSettings(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"no brokers", Config{}, "brokers is empty"},
		{"blank broker", Config{Brokers: []string{""}}, "brokers[0] is blank"},
		{"unknown sasl mechanism", Config{Brokers: []string{"k:9092"}, SASL: SASLConfig{Mechanism: "OAUTHBEARER"}}, "sasl.mechanism"},
		{"sasl without credentials", Config{Brokers: []string{"k:9092"}, SASL: SASLConfig{Mechanism: "PLAIN"}}, "sasl.username"},
		{"cert without key", Config{Brokers: []string{"k:9092"}, TLS: TLSConfig{Enabled: true, CertFile: "c.pem"}}, "tls.key_file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ClientOptions(tt.cfg)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("ClientOptions error = %v, want it to contain %q", err, tt.want)
			}
		})
	}
}

// testPKI is a throwaway CA with a server certificate for the loopback
// address and localhost, and a client certificate, all written to files.
type testPKI struct {
	caFile, certFile, keyFile string // CA, client certificate and its key
	caPool                    *x509.CertPool
	server                    tls.Certificate
}

func newTestPKI(t *testing.T) testPKI {
	t.Helper()
	dir := t.TempDir()
	now := time.Now()
	newKey := func() *ecdsa.PrivateKey {
		key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		return key
	}
	writePEM := func(name, blockType string, der []byte) string {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: der}), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return path
	}

	caKey := newKey()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "shadowtap test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA: %v", err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}

	issue := func(serial int64, usage x509.ExtKeyUsage, dnsNames []string, ips []net.IP) (certDER, keyDER []byte) {
		key := newKey()
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: "shadowtap test"},
			NotBefore:    now.Add(-time.Hour),
			NotAfter:     now.Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
			DNSNames:     dnsNames,
			IPAddresses:  ips,
		}
		certDER, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
		if err != nil {
			t.Fatalf("issue certificate: %v", err)
		}
		keyDER, err = x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			t.Fatalf("marshal key: %v", err)
		}
		return certDER, keyDER
	}

	serverDER, serverKeyDER := issue(2, x509.ExtKeyUsageServerAuth, []string{"localhost"}, []net.IP{net.IPv4(127, 0, 0, 1)})
	server, err := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: serverDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: serverKeyDER}),
	)
	if err != nil {
		t.Fatalf("server key pair: %v", err)
	}
	clientDER, clientKeyDER := issue(3, x509.ExtKeyUsageClientAuth, nil, nil)

	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return testPKI{
		caFile:   writePEM("ca.pem", "CERTIFICATE", caDER),
		certFile: writePEM("client.pem", "CERTIFICATE", clientDER),
		keyFile:  writePEM("client-key.pem", "PRIVATE KEY", clientKeyDER),
		caPool:   pool,
		server:   server,
	}
}

func TestClientOptionsTLS(t *testing.T) {
	pki := newTestPKI(t)
	// The broker only accepts clients with a certificate from the test CA.
	cluster := newCluster(t, kfake.TLS(&tls.Config{
		Certificates: []tls.Certificate{pki.server},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pki.caPool,
		MinVersion:   tls.VersionTLS12,
	}))
	brokers := cluster.ListenAddrs()
	mTLS := TLSConfig{Enabled: true, CAFile: pki.caFile, CertFile: pki.certFile, KeyFile: pki.keyFile}

	t.Run("options", func(t *testing.T) {
		tlsCfg := mTLS
		tlsCfg.ServerName = "localhost"
		cl := newOptionsClient(t, Config{Brokers: brokers, TLS: tlsCfg})
		got, ok := cl.OptValue(kgo.DialTLSConfig).(*tls.Config)
		if !ok || got == nil {
			t.Fatalf("DialTLSConfig = %v, want a TLS config", cl.OptValue(kgo.DialTLSConfig))
		}
		if got.MinVersion != tls.VersionTLS12 {
			t.Errorf("MinVersion = %x, want TLS 1.2", got.MinVersion)
		}
		if got.ServerName != "localhost" {
			t.Errorf("ServerName = %q, want localhost", got.ServerName)
		}
		if got.RootCAs == nil || !got.RootCAs.Equal(pki.caPool) {
			t.Error("RootCAs is not the CA from ca_file")
		}
		if len(got.Certificates) != 1 {
			t.Errorf("got %d client certificates, want 1", len(got.Certificates))
		}
		if got.InsecureSkipVerify {
			t.Error("InsecureSkipVerify is set")
		}
	})

	handshakes := []struct {
		name    string
		tls     func() TLSConfig
		wantErr bool
	}{
		{"mutual TLS", func() TLSConfig { return mTLS }, false},
		{"server name matches DNS SAN", func() TLSConfig { c := mTLS; c.ServerName = "localhost"; return c }, false},
		{"server name mismatch", func() TLSConfig { c := mTLS; c.ServerName = "kafka.example.com"; return c }, true},
		{"system roots do not trust the test CA", func() TLSConfig { c := mTLS; c.CAFile = ""; return c }, true},
		{"insecure skip verify without CA", func() TLSConfig { c := mTLS; c.CAFile, c.InsecureSkipVerify = "", true; return c }, false},
		{"no client certificate", func() TLSConfig { c := mTLS; c.CertFile, c.KeyFile = "", ""; return c }, true},
		{"plaintext to a TLS listener", func() TLSConfig { return TLSConfig{} }, true},
	}
	for _, tt := range handshakes {
		t.Run(tt.name, func(t *testing.T) {
			cl := newOptionsClient(t, Config{Brokers: brokers, TLS: tt.tls()})
			err := ping(cl)
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("Ping = %v, want error: %v", err, tt.wantErr)
			}
		})
	}

	t.Run("bad files", func(t *testing.T) {
		garbage := filepath.Join(t.TempDir(), "garbage.pem")
		if err := os.WriteFile(garbage, []byte("not a certificate"), 0o600); err != nil {
			t.Fatal(err)
		}
		tests := []struct {
			name string
			tls  TLSConfig
			want string
		}{
			{"missing CA file", TLSConfig{Enabled: true, CAFile: filepath.Join(t.TempDir(), "missing.pem")}, "tls.ca_file"},
			{"CA file without certificates", TLSConfig{Enabled: true, CAFile: garbage}, "no PEM certificates"},
			{"key does not parse", TLSConfig{Enabled: true, CertFile: pki.certFile, KeyFile: garbage}, "client certificate"},
			{"key does not match certificate", TLSConfig{Enabled: true, CertFile: pki.caFile, KeyFile: pki.keyFile}, "client certificate"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				_, err := ClientOptions(Config{Brokers: brokers, TLS: tt.tls})
				if err == nil || !strings.Contains(err.Error(), tt.want) {
					t.Fatalf("ClientOptions error = %v, want it to contain %q", err, tt.want)
				}
			})
		}
	})
}

func TestClientOptionsSASL(t *testing.T) {
	tests := []struct {
		mechanism string // as configured
		canonical string // as sent to Kafka
	}{
		{"PLAIN", "PLAIN"},
		{"scram-sha-256", "SCRAM-SHA-256"},
		{" SCRAM-SHA-512 ", "SCRAM-SHA-512"},
	}
	for _, tt := range tests {
		t.Run(tt.canonical, func(t *testing.T) {
			cluster := newCluster(t, kfake.EnableSASL(), kfake.Superuser(tt.canonical, "shadowtap", "s3cret"))
			cfg := Config{
				Brokers: cluster.ListenAddrs(),
				SASL:    SASLConfig{Mechanism: tt.mechanism, Username: "shadowtap", Password: "s3cret"},
			}

			cl := newOptionsClient(t, cfg)
			mechs, _ := cl.OptValue(kgo.SASL).([]sasl.Mechanism)
			if len(mechs) != 1 || mechs[0].Name() != tt.canonical {
				t.Fatalf("SASL mechanisms = %v, want one %s", mechs, tt.canonical)
			}
			if err := ping(cl); err != nil {
				t.Fatalf("Ping with valid credentials: %v", err)
			}

			cfg.SASL.Password = "wrong"
			if err := ping(newOptionsClient(t, cfg)); err == nil {
				t.Fatal("Ping with a wrong password succeeded")
			}
		})
	}
}
