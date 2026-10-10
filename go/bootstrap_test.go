package xrpc

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSharedBootstrapCorpus(t *testing.T) {
	data, err := os.ReadFile("../contracts/fixtures/bootstrap.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Cases []struct {
			Name    string          `json:"name"`
			Binding json.RawMessage `json:"binding"`
			Valid   bool            `json:"valid"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, test := range corpus.Cases {
		t.Run(test.Name, func(t *testing.T) {
			_, err := parseBootstrapBinding(test.Binding)
			if (err == nil) != test.Valid {
				t.Fatalf("valid=%v err=%v", test.Valid, err)
			}
		})
	}
}
func localBinding() BootstrapBinding {
	return BootstrapBinding{SchemaVersion: 1, TargetID: "local", Service: "fixture", APIVersion: "v1", Profile: HTTP, Endpoint: Endpoint{Kind: "unix", Address: "/private/runtime/rpc.sock"}, RuntimeGrant: "runtime", Authentication: LocalPrivate, SecretHandles: SecretHandles{}, StorageGrants: []string{}}
}
func TestBootstrapStrictIdentityAndRequiredFields(t *testing.T) {
	data, _ := json.Marshal(localBinding())
	if _, err := parseBootstrapBinding(data); err != nil {
		t.Fatal(err)
	}
	bad := []string{strings.Replace(string(data), `"schema_version":1`, `"schema_version":1,"schema_version":1`, 1), strings.Replace(string(data), `"secret_handles":{}`, `"secret_handles":null`, 1), strings.Replace(string(data), `"storage_grants":[]`, `"storage_grants":null`, 1), string(data) + ` {}`,
		strings.Replace(string(data), `"authentication":"local_private"`, `"authentication":"local_private","Authentication":"server_tls"`, 1),
		strings.Replace(string(data), `"secret_handles":{}`, `"secret_handles":{"authorization":""}`, 1),
		strings.Replace(string(data), `"secret_handles":{}`, `"secret_handles":{"authorization":null}`, 1),
		strings.Replace(string(data), `"secret_handles":{}`, `"secret_handles":{"Authorization":"caller"}`, 1),
		strings.Repeat(" ", MaxBootstrapBytes) + string(data)}
	for _, value := range bad {
		if _, err := parseBootstrapBinding([]byte(value)); err == nil {
			t.Fatal("malformed input accepted")
		}
	}
	b := localBinding()
	b.TargetID = "target space"
	if b.Validate() == nil {
		t.Fatal("noncanonical name")
	}
	b = localBinding()
	b.Endpoint = Endpoint{Kind: "tls", Address: "localhost:08443"}
	b.Profile = GRPC
	b.Authentication = MutualTLS
	b.SecretHandles = SecretHandles{"identity", "trust", "caller"}
	if b.Validate() == nil {
		t.Fatal("leading-zero port")
	}
	for _, address := range []string{"https://localhost:8443/", "https://localhost:", "https://localhost#", "https://localhost?", "https://localhost:0", "https://localhost:65536"} {
		b.Profile = HTTP
		b.Endpoint = Endpoint{Kind: "https", Address: address}
		if b.Validate() == nil {
			t.Fatalf("nonorigin accepted %s", address)
		}
	}
	first, _ := NewInstanceID()
	second, _ := NewInstanceID()
	if !ValidID(first) || first == second {
		t.Fatal("instance identity")
	}
}
func TestPrivateBootstrapFilesRejectWithoutBlocking(t *testing.T) {
	directory := t.TempDir()
	if err := os.Chmod(directory, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "input")
	if err := os.WriteFile(path, []byte("1234"), 0600); err != nil {
		t.Fatal(err)
	}
	if data, err := readPrivateBootstrapFile(path, 4); err != nil || string(data) != "1234" {
		t.Fatal(err)
	}
	if _, err := readPrivateBootstrapFile(path, 3); err == nil {
		t.Fatal("overflow")
	}
	link := filepath.Join(directory, "link")
	_ = os.Symlink(path, link)
	if _, err := readPrivateBootstrapFile(link, 4); err == nil {
		t.Fatal("symlink")
	}
	_ = os.Remove(link)
	if err := os.Link(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readPrivateBootstrapFile(path, 4); err == nil {
		t.Fatal("hard link")
	}
	_ = os.Remove(link)
	_ = os.Chmod(path, 0644)
	if _, err := readPrivateBootstrapFile(path, 4); err == nil {
		t.Fatal("public file")
	}
	_ = os.Chmod(path, 0600)
	_ = os.Chmod(directory, 0755)
	if _, err := readPrivateBootstrapFile(path, 4); err == nil {
		t.Fatal("public parent")
	}
	_ = os.Chmod(directory, 0700)
	fifo := filepath.Join(directory, "fifo")
	if err := unix.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := readPrivateBootstrapFile(fifo, 4); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO")
		}
	case <-time.After(time.Second):
		t.Fatal("FIFO startup blocked")
	}
}
func bootstrapMaterial(t *testing.T) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "fixture"}, DNSNames: []string{"fixture.example"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded})
}
func TestBootstrapInputLoadsOneCredentialSnapshot(t *testing.T) {
	directory := t.TempDir()
	_ = os.Chmod(directory, 0700)
	cert, key := bootstrapMaterial(t)
	write := func(name string, data []byte) string {
		path := filepath.Join(directory, name)
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	certPath, keyPath, caPath, tokenPath := write("cert", cert), write("key", key), write("ca", cert), write("token", []byte("one-token"))
	b := localBinding()
	b.Endpoint = Endpoint{Kind: "https", Address: "https://fixture.example:8443"}
	b.Authentication = MutualTLS
	b.SecretHandles = SecretHandles{"identity", "trust", "caller"}
	b.StorageGrants = []string{"opaque:storage"}
	document := map[string]any{"schema_version": 1, "binding": b, "grants": map[string]any{"identity": map[string]string{"kind": "tls_identity", "cert_file": certPath, "key_file": keyPath}, "trust": map[string]string{"kind": "tls_trust", "ca_file": caPath}, "caller": map[string]string{"kind": "bearer", "token_file": tokenPath}}, "application": map[string]any{"domain": "opaque", "array": []int{1, 2}}}
	encoded, _ := json.Marshal(document)
	path := write("input", encoded)
	input, err := LoadBootstrapInput(path, BootstrapClient)
	if err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(tokenPath, []byte("replacement"), 0600)
	_ = os.Remove(keyPath)
	if input.Credentials().Headers()["Authorization"] != "Bearer one-token" {
		t.Fatal("credential reread")
	}
	metadata := input.Binding()
	metadata.StorageGrants[0] = "changed"
	application := input.Application()
	application[0] = 'X'
	if input.Binding().StorageGrants[0] != "opaque:storage" || !json.Valid(input.Application()) {
		t.Fatal("startup metadata mutable")
	}
	tlsConfig := input.Credentials().TLSConfig()
	tlsConfig.Certificates[0].PrivateKey.(*ecdsa.PrivateKey).D.SetInt64(1)
	if input.Credentials().TLSConfig().Certificates[0].PrivateKey.(*ecdsa.PrivateKey).D.Int64() == 1 {
		t.Fatal("private key snapshot mutable")
	}
	if _, err := newTLSTrustGrant(append([]byte("junk"), cert...)); err == nil {
		t.Fatal("impure CA accepted")
	}
	if _, err := newTLSTrustGrant(append([]byte("-----BEGIN CERTIFICATE-----\n!\n-----END CERTIFICATE-----\n"), cert...)); err == nil {
		t.Fatal("invalid PEM block skipped")
	}
	for _, token := range []string{"token\n", "=", "one=two", "", strings.Repeat("x", 1025)} {
		if _, err := newBearerGrant(token); err == nil {
			t.Fatal("invalid bearer accepted")
		}
	}
}
func TestLocalBootstrapInputHasNoSyntheticCredentials(t *testing.T) {
	directory := t.TempDir()
	_ = os.Chmod(directory, 0700)
	b := localBinding()
	write := func(application json.RawMessage) string {
		document := map[string]any{"schema_version": 1, "binding": b, "grants": map[string]any{}, "application": application}
		data, _ := json.Marshal(document)
		path := filepath.Join(directory, "input")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	input, err := LoadBootstrapInput(write(json.RawMessage(`{"domain":true}`)), BootstrapServer)
	if err != nil {
		t.Fatal(err)
	}
	if input.Credentials().TLSConfig() != nil || len(input.Credentials().Headers()) != 0 {
		t.Fatal("local lease requires synthetic credentials")
	}
	application := json.RawMessage(strings.Repeat("[", 33) + "0" + strings.Repeat("]", 33))
	if _, err := LoadBootstrapInput(write(application), BootstrapServer); err == nil {
		t.Fatal("application nesting")
	}
	application = json.RawMessage(strings.Repeat("[", 32) + "0" + strings.Repeat("]", 32))
	if _, err := LoadBootstrapInput(write(application), BootstrapServer); err != nil {
		t.Fatal("bounded application refused", err)
	}
}

func TestBootstrapLoaderRejectsMalformedGrantDescriptors(t *testing.T) {
	directory := t.TempDir()
	_ = os.Chmod(directory, 0700)
	token := filepath.Join(directory, "token")
	if err := os.WriteFile(token, []byte("token\n"), 0600); err != nil {
		t.Fatal(err)
	}
	binding := localBinding()
	binding.SecretHandles = SecretHandles{Authorization: "caller"}
	for _, descriptor := range []map[string]any{
		{"kind": "bearer", "token_file": token},
		{"kind": "bearer", "token_file": token, "cert_file": ""},
		{"kind": "bearer", "Token_File": token},
		{"kind": "tls_identity", "cert_file": token, "key_file": token},
		{"kind": "tls_trust", "ca_file": token},
	} {
		document := map[string]any{"schema_version": 1, "binding": binding, "grants": map[string]any{"caller": descriptor}}
		data, _ := json.Marshal(document)
		path := filepath.Join(directory, "input")
		if err := os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadBootstrapInput(path, BootstrapServer); err == nil {
			t.Fatal("malformed grant accepted")
		}
	}
	if err := os.WriteFile(token, []byte("valid-token"), 0600); err != nil {
		t.Fatal(err)
	}
	document := map[string]any{"schema_version": 1, "binding": binding, "grants": map[string]any{"caller": map[string]string{"kind": "bearer", "token_file": token}}, "application": json.RawMessage(`{"large_number":1e10000}`)}
	data, _ := json.Marshal(document)
	path := filepath.Join(directory, "input")
	_ = os.WriteFile(path, data, 0600)
	input, err := LoadBootstrapInput(path, BootstrapServer)
	if err != nil || string(input.Application()) != `{"large_number":1e10000}` {
		t.Fatal("opaque number interpreted", err)
	}
	binding.SecretHandles = SecretHandles{TLSIdentity: "missing"}
	if _, err := binding.resolveCredentials(func(string) (credentialGrant, error) { return credentialGrant{}, bootstrapError() }, BootstrapServer); err == nil {
		t.Fatal("local named grant not resolved")
	}
}
