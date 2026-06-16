package fencing

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	weftv1 "github.com/openweft/weft-proto"
)

func TestLoadClientTLSConfig_EmptyCA_Refuses(t *testing.T) {
	if _, err := LoadClientTLSConfig("", "", "", ""); err == nil {
		t.Fatal("LoadClientTLSConfig with empty CA path should error")
	}
}

func TestLoadClientTLSConfig_CAOnly(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeTestCA(t, caPath)

	cfg, err := LoadClientTLSConfig(caPath, "", "", "")
	if err != nil {
		t.Fatalf("LoadClientTLSConfig: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Error("RootCAs not populated")
	}
	if cfg.MinVersion < 0x303 {
		t.Errorf("MinVersion = %x; want >= TLS 1.2", cfg.MinVersion)
	}
	if len(cfg.Certificates) != 0 {
		t.Errorf("Certificates should be empty without an mTLS pair; got %d", len(cfg.Certificates))
	}
}

func TestLoadClientTLSConfig_MTLSRequiresBoth(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeTestCA(t, caPath)

	if _, err := LoadClientTLSConfig(caPath, filepath.Join(dir, "client.pem"), "", ""); err == nil {
		t.Error("cert without key should error")
	}
	if _, err := LoadClientTLSConfig(caPath, "", filepath.Join(dir, "client.key"), ""); err == nil {
		t.Error("key without cert should error")
	}
}

func TestLoadClientTLSConfig_MTLSPair(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client.key")
	writeTestCA(t, caPath)
	writeTestKeyPair(t, certPath, keyPath)

	cfg, err := LoadClientTLSConfig(caPath, certPath, keyPath, "")
	if err != nil {
		t.Fatalf("LoadClientTLSConfig with mTLS pair: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(cfg.Certificates))
	}
}

func TestLoadClientTLSConfig_BadKeyPair(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeTestCA(t, caPath)
	certPath := filepath.Join(dir, "client.pem")
	keyPath := filepath.Join(dir, "client.key")
	if err := os.WriteFile(certPath, []byte("not a cert"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadClientTLSConfig(caPath, certPath, keyPath, ""); err == nil {
		t.Error("garbage key pair should error")
	}
}

func TestLoadClientTLSConfig_MissingCAFile(t *testing.T) {
	if _, err := LoadClientTLSConfig(filepath.Join(t.TempDir(), "nope.pem"), "", "", ""); err == nil {
		t.Error("missing CA file should error")
	}
}

func TestLoadClientTLSConfig_BadCA(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, []byte("garbage not a PEM"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadClientTLSConfig(caPath, "", "", ""); err == nil {
		t.Error("garbage PEM should error")
	}
}

func TestLoadClientTLSConfig_ServerName(t *testing.T) {
	dir := t.TempDir()
	caPath := filepath.Join(dir, "ca.pem")
	writeTestCA(t, caPath)
	cfg, err := LoadClientTLSConfig(caPath, "", "", "weft-agent.dc1.internal")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServerName != "weft-agent.dc1.internal" {
		t.Errorf("ServerName = %q; want weft-agent.dc1.internal", cfg.ServerName)
	}
}

// TestNewGRPCStopperTLS_NilFalls back to insecure and stays usable.
func TestNewGRPCStopperTLS_NilFallsBack(t *testing.T) {
	s := NewGRPCStopperTLS("127.0.0.1:1", "proj", nil, quietLog())
	if s == nil {
		t.Fatal("nil")
	}
	if s.tls != nil {
		t.Error("nil tls config should fall back to insecure (tls == nil)")
	}
}

func TestGRPCStopper_CloseNoConn(t *testing.T) {
	s := NewGRPCStopper("127.0.0.1:1", "proj", quietLog())
	if err := s.Close(); err != nil {
		t.Errorf("Close with no conn should be nil: %v", err)
	}
}

func TestGRPCStopper_NilLogDefaults(t *testing.T) {
	if NewGRPCStopper("a:1", "p", nil).log == nil {
		t.Error("NewGRPCStopper nil log must default")
	}
	if NewGRPCStopperTLS("a:1", "p", &tls.Config{MinVersion: tls.VersionTLS12}, nil).log == nil {
		t.Error("NewGRPCStopperTLS nil log must default")
	}
}

func TestIsStopped(t *testing.T) {
	stopped := []weftv1.VMState{
		weftv1.VMState_VM_STATE_STOPPED,
		weftv1.VMState_VM_STATE_NOT_CREATED,
		weftv1.VMState_VM_STATE_ERROR,
	}
	for _, s := range stopped {
		if !isStopped(s) {
			t.Errorf("isStopped(%v) = false; want true", s)
		}
	}
	// Any other state (e.g. running) is not stopped.
	if isStopped(weftv1.VMState(99)) {
		t.Error("isStopped(unknown) = true; want false")
	}
}

// --- test fixtures ---

func writeTestCA(t *testing.T, path string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(path, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeTestKeyPair(t *testing.T, certPath, keyPath string) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-client"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
}
