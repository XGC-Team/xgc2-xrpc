package xrpc

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"net"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf8"
)

const MaxBootstrapBytes = 16 << 10
const (
	LocalPrivate = "local_private"
	ServerTLS    = "server_tls"
	MutualTLS    = "mutual_tls"
)

type BootstrapRole string

const (
	BootstrapServer BootstrapRole = "server"
	BootstrapClient BootstrapRole = "client"
)

type SecretHandles struct {
	TLSIdentity   string `json:"tls_identity,omitempty"`
	TLSTrust      string `json:"tls_trust,omitempty"`
	Authorization string `json:"authorization,omitempty"`
}

// BootstrapBinding is explicit startup metadata for an actual service owner.
// Runtime and storage handles remain opaque; the SDK does not manage grants.
type BootstrapBinding struct {
	SchemaVersion  int           `json:"schema_version"`
	TargetID       string        `json:"target_id"`
	Service        string        `json:"service"`
	APIVersion     string        `json:"api_version"`
	Profile        string        `json:"profile"`
	Endpoint       Endpoint      `json:"endpoint"`
	RuntimeGrant   string        `json:"runtime_grant"`
	Authentication string        `json:"authentication"`
	SecretHandles  SecretHandles `json:"secret_handles"`
	StorageGrants  []string      `json:"storage_grants"`
}

func bootstrapError() error { return errors.New("xrpc: invalid explicit bootstrap input") }
func (b BootstrapBinding) Validate() error {
	if b.SchemaVersion != 1 || !ValidID(b.TargetID) || !ValidID(b.Service) || !ValidID(b.APIVersion) || !ValidID(b.RuntimeGrant) || b.StorageGrants == nil || len(b.StorageGrants) > 32 {
		return bootstrapError()
	}
	seen := make(map[string]bool, len(b.StorageGrants))
	for _, grant := range b.StorageGrants {
		if !ValidID(grant) || seen[grant] {
			return bootstrapError()
		}
		seen[grant] = true
	}
	if b.Profile != HTTP && b.Profile != GRPC {
		return bootstrapError()
	}
	if err := b.Endpoint.Validate(); err != nil || len(b.Endpoint.Address) > 2048 {
		return bootstrapError()
	}
	for _, c := range b.Endpoint.Address {
		if c <= 32 || c == 127 {
			return bootstrapError()
		}
	}
	if b.Profile == HTTP && b.Endpoint.Kind == "tls" || b.Profile == GRPC && b.Endpoint.Kind == "https" {
		return bootstrapError()
	}
	if b.Endpoint.Kind == "https" {
		u, err := url.Parse(b.Endpoint.Address)
		if err != nil || u.Path != "" || u.RawPath != "" || u.ForceQuery || u.Opaque != "" || u.RawFragment != "" || !validBootstrapHost(u.Hostname()) {
			return bootstrapError()
		}
		if u.Port() != "" && !validBootstrapPort(u.Port()) {
			return bootstrapError()
		}
		host := u.Hostname()
		if u.Port() != "" {
			host = net.JoinHostPort(host, u.Port())
		} else if strings.Contains(host, ":") {
			host = "[" + host + "]"
		}
		if b.Endpoint.Address != "https://"+host {
			return bootstrapError()
		}
	}
	if b.Endpoint.Kind == "tls" {
		host, port, err := net.SplitHostPort(b.Endpoint.Address)
		if err != nil || !validBootstrapHost(host) || !validBootstrapPort(port) || net.JoinHostPort(host, port) != b.Endpoint.Address {
			return bootstrapError()
		}
	}
	names := []string{b.SecretHandles.TLSIdentity, b.SecretHandles.TLSTrust, b.SecretHandles.Authorization}
	handles := map[string]bool{}
	for _, name := range names {
		if name != "" {
			if !ValidID(name) || handles[name] {
				return bootstrapError()
			}
			handles[name] = true
		}
	}
	if b.Endpoint.Kind == "unix" {
		if b.Authentication != LocalPrivate {
			return bootstrapError()
		}
	} else {
		if b.Authentication != ServerTLS && b.Authentication != MutualTLS {
			return bootstrapError()
		}
		for _, name := range names {
			if name == "" {
				return bootstrapError()
			}
		}
	}
	return nil
}
func validBootstrapPort(value string) bool {
	n, err := strconv.Atoi(value)
	return err == nil && n >= 1 && n <= 65535 && strconv.Itoa(n) == value
}
func validBootstrapHost(host string) bool {
	if host == "" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String() == host
	}
	if len(host) > 253 || host != strings.ToLower(host) {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
		for _, c := range label {
			if c != '-' && (c < 'a' || c > 'z') && (c < '0' || c > '9') {
				return false
			}
		}
	}
	return true
}
func cloneBinding(b BootstrapBinding) BootstrapBinding {
	b.StorageGrants = append([]string{}, b.StorageGrants...)
	return b
}
func (b BootstrapBinding) ServiceRef(instanceID string) (ServiceRef, error) {
	if err := b.Validate(); err != nil {
		return ServiceRef{}, err
	}
	if !ValidID(instanceID) {
		return ServiceRef{}, bootstrapError()
	}
	return ServiceRef{TargetID: b.TargetID, Service: b.Service, APIVersion: b.APIVersion, InstanceID: instanceID, Profile: b.Profile, Endpoint: b.Endpoint}, nil
}
func NewInstanceID() (string, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return "", errors.New("xrpc: instance identity unavailable")
	}
	return hex.EncodeToString(id[:]), nil
}

// Strict parsing rejects repeated keys, unknown fields, missing/null required
// objects and arrays. Application JSON is opaque and never decoded by adapters.
func strictJSON(data []byte, destination any) error {
	if len(data) == 0 || len(data) > MaxBootstrapBytes || !utf8.Valid(data) {
		return bootstrapError()
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanJSON(decoder, 0); err != nil {
		return bootstrapError()
	}
	if _, err := decoder.Token(); err != io.EOF {
		return bootstrapError()
	}
	if err := exactJSONFields(data, reflect.TypeOf(destination)); err != nil {
		return bootstrapError()
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return bootstrapError()
	}
	return nil
}
func scanJSON(d *json.Decoder, depth int) error {
	if depth > 36 {
		return bootstrapError()
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return bootstrapError()
			}
			seen[name] = true
			if err := scanJSON(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := scanJSON(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return bootstrapError()
	}
	_, err = d.Token()
	return err
}

// encoding/json accepts case-folded struct keys. Startup contracts require
// exact keys, including nested objects, so reject aliases before decoding.
func exactJSONFields(data []byte, typ reflect.Type) error {
	if typ == nil {
		return bootstrapError()
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ == reflect.TypeOf(json.RawMessage{}) {
		return nil
	}
	switch typ.Kind() {
	case reflect.Struct:
		var object map[string]json.RawMessage
		if json.Unmarshal(data, &object) != nil || object == nil {
			return bootstrapError()
		}
		fields := make(map[string]reflect.Type, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name != "" && name != "-" {
				fields[name] = field.Type
			}
		}
		for name, value := range object {
			field, ok := fields[name]
			if !ok {
				return bootstrapError()
			}
			if err := exactJSONFields(value, field); err != nil {
				return err
			}
		}
	case reflect.Map:
		var object map[string]json.RawMessage
		if json.Unmarshal(data, &object) != nil {
			return bootstrapError()
		}
		for _, value := range object {
			if err := exactJSONFields(value, typ.Elem()); err != nil {
				return err
			}
		}
	case reflect.Slice, reflect.Array:
		var array []json.RawMessage
		if json.Unmarshal(data, &array) != nil {
			return bootstrapError()
		}
		for _, value := range array {
			if err := exactJSONFields(value, typ.Elem()); err != nil {
				return err
			}
		}
	}
	return nil
}
func requiredJSON(data []byte, names ...string) bool {
	if len(data) == 0 || len(data) > MaxBootstrapBytes || !utf8.Valid(data) {
		return false
	}
	var values map[string]json.RawMessage
	if json.Unmarshal(data, &values) != nil || values == nil {
		return false
	}
	for _, name := range names {
		value, ok := values[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return false
		}
	}
	return true
}
func ParseBootstrapBinding(data []byte) (BootstrapBinding, error) {
	var b BootstrapBinding
	if !requiredJSON(data, "schema_version", "target_id", "service", "api_version", "profile", "endpoint", "runtime_grant", "authentication", "secret_handles", "storage_grants") {
		return b, bootstrapError()
	}
	if err := strictJSON(data, &b); err != nil {
		return b, err
	}
	var fields map[string]json.RawMessage
	_ = json.Unmarshal(data, &fields)
	var handles map[string]json.RawMessage
	_ = json.Unmarshal(fields["secret_handles"], &handles)
	for _, raw := range handles {
		var handle string
		if json.Unmarshal(raw, &handle) != nil || !ValidID(handle) {
			return BootstrapBinding{}, bootstrapError()
		}
	}
	if err := b.Validate(); err != nil {
		return BootstrapBinding{}, err
	}
	return cloneBinding(b), nil
}

// Authorization receives exactly the native caller header values. A grant
// owner may implement existing peer policy; authorization cannot extend ctx.
type Authorization func(context.Context, []string) bool
type CredentialGrant struct {
	kind      string
	identity  tls.Certificate
	trust     *x509.CertPool
	authorize Authorization
	headers   map[string]string
}
type GrantResolver func(string) (CredentialGrant, error)

func (g CredentialGrant) Kind() string { return g.kind }
func NewTLSIdentityGrant(certPEM, keyPEM []byte) (CredentialGrant, error) {
	if len(certPEM) == 0 || len(certPEM) > 128<<10 || len(keyPEM) == 0 || len(keyPEM) > 64<<10 {
		return CredentialGrant{}, bootstrapError()
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil || len(cert.Certificate) == 0 {
		return CredentialGrant{}, bootstrapError()
	}
	for _, der := range cert.Certificate {
		if _, err := x509.ParseCertificate(der); err != nil {
			return CredentialGrant{}, bootstrapError()
		}
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return CredentialGrant{}, bootstrapError()
	}
	signer, ok := cert.PrivateKey.(crypto.Signer)
	if !ok {
		return CredentialGrant{}, bootstrapError()
	}
	expected, _ := x509.MarshalPKIXPublicKey(leaf.PublicKey)
	actual, err := x509.MarshalPKIXPublicKey(signer.Public())
	if err != nil || !bytes.Equal(expected, actual) {
		return CredentialGrant{}, bootstrapError()
	}
	cert.Leaf = leaf
	return CredentialGrant{kind: "tls_identity", identity: cert}, nil
}
func NewTLSTrustGrant(caPEM []byte) (CredentialGrant, error) {
	if len(caPEM) == 0 || len(caPEM) > 128<<10 {
		return CredentialGrant{}, bootstrapError()
	}
	pool := x509.NewCertPool()
	count := 0
	for len(bytes.TrimSpace(caPEM)) > 0 {
		caPEM = bytes.TrimSpace(caPEM)
		if !bytes.HasPrefix(caPEM, []byte("-----BEGIN CERTIFICATE-----")) {
			return CredentialGrant{}, bootstrapError()
		}
		block, rest := pem.Decode(caPEM)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return CredentialGrant{}, bootstrapError()
		}
		consumed := caPEM[:len(caPEM)-len(rest)]
		if bytes.Count(consumed, []byte("-----BEGIN CERTIFICATE-----")) != 1 || bytes.Count(consumed, []byte("-----END CERTIFICATE-----")) != 1 {
			return CredentialGrant{}, bootstrapError()
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return CredentialGrant{}, bootstrapError()
		}
		pool.AddCert(cert)
		count++
		caPEM = rest
	}
	if count == 0 {
		return CredentialGrant{}, bootstrapError()
	}
	return CredentialGrant{kind: "tls_trust", trust: pool}, nil
}
func NewAuthorizationGrant(authorize Authorization, headers map[string]string) (CredentialGrant, error) {
	if authorize == nil || len(headers) > 1 {
		return CredentialGrant{}, bootstrapError()
	}
	copy := map[string]string{}
	for name, value := range headers {
		if strings.ToLower(name) != "authorization" || value == "" || len(value) > 1031 || strings.ContainsAny(value, "\r\n\x00") {
			return CredentialGrant{}, bootstrapError()
		}
		copy["Authorization"] = value
	}
	return CredentialGrant{kind: "authorization", authorize: authorize, headers: copy}, nil
}
func NewBearerGrant(token string) (CredentialGrant, error) {
	if len(token) == 0 || len(token) > 1024 {
		return CredentialGrant{}, bootstrapError()
	}
	base, ending := false, false
	for _, c := range token {
		if c == '=' {
			ending = true
			continue
		}
		if ending || !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("._~+/-", c)) {
			return CredentialGrant{}, bootstrapError()
		}
		base = true
	}
	if !base {
		return CredentialGrant{}, bootstrapError()
	}
	expected := sha256.Sum256([]byte("Bearer " + token))
	return NewAuthorizationGrant(func(ctx context.Context, values []string) bool {
		if ctx == nil || ctx.Err() != nil || len(values) != 1 || len(values[0]) > 1031 {
			return false
		}
		actual := sha256.Sum256([]byte(values[0]))
		return subtle.ConstantTimeCompare(actual[:], expected[:]) == 1
	}, map[string]string{"Authorization": "Bearer " + token})
}

type BootstrapCredentials struct {
	binding       BootstrapBinding
	role          BootstrapRole
	tls           *tls.Config
	authorization Authorization
	headers       map[string]string
}

func (b BootstrapBinding) ResolveCredentials(resolve GrantResolver, role BootstrapRole) (*BootstrapCredentials, error) {
	if err := b.Validate(); err != nil {
		return nil, err
	}
	if role != BootstrapServer && role != BootstrapClient {
		return nil, bootstrapError()
	}
	result := &BootstrapCredentials{binding: cloneBinding(b), role: role, headers: map[string]string{}}
	handles := b.SecretHandles
	if b.Endpoint.Kind != "unix" || handles.TLSIdentity != "" || handles.TLSTrust != "" || handles.Authorization != "" {
		if resolve == nil {
			return nil, bootstrapError()
		}
	}
	if b.Endpoint.Kind == "unix" {
		for _, field := range []struct{ handle, kind string }{{handles.TLSIdentity, "tls_identity"}, {handles.TLSTrust, "tls_trust"}} {
			if field.handle != "" {
				grant, err := resolve(field.handle)
				if err != nil || grant.kind != field.kind {
					return nil, bootstrapError()
				}
			}
		}
	}
	if b.Endpoint.Kind != "unix" {
		identity, err := resolve(handles.TLSIdentity)
		if err != nil || identity.kind != "tls_identity" || len(identity.identity.Certificate) == 0 {
			return nil, bootstrapError()
		}
		trust, err := resolve(handles.TLSTrust)
		if err != nil || trust.kind != "tls_trust" || trust.trust == nil {
			return nil, bootstrapError()
		}
		config := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cloneCertificate(identity.identity)}}
		if role == BootstrapServer {
			config.ClientCAs = trust.trust.Clone()
			if b.Authentication == MutualTLS {
				config.ClientAuth = tls.RequireAndVerifyClientCert
			}
		} else {
			config.RootCAs = trust.trust.Clone()
			if b.Endpoint.Kind == "https" {
				u, _ := url.Parse(b.Endpoint.Address)
				config.ServerName = u.Hostname()
			} else {
				config.ServerName, _, _ = net.SplitHostPort(b.Endpoint.Address)
			}
		}
		result.tls = config
	}
	if handles.Authorization != "" {
		auth, err := resolve(handles.Authorization)
		if err != nil || auth.kind != "authorization" || auth.authorize == nil {
			return nil, bootstrapError()
		}
		if role == BootstrapClient && len(auth.headers) != 1 {
			return nil, bootstrapError()
		}
		result.authorization = auth.authorize
		for key, value := range auth.headers {
			result.headers[key] = value
		}
	}
	return result, nil
}
func cloneCertificate(c tls.Certificate) tls.Certificate {
	// Constructors accept only parsed PEM private keys. Copy the native key as
	// well as DER so a returned TLS config cannot mutate the startup owner.
	if encoded, err := x509.MarshalPKCS8PrivateKey(c.PrivateKey); err == nil {
		if key, err := x509.ParsePKCS8PrivateKey(encoded); err == nil {
			c.PrivateKey = key
		}
		clear(encoded)
	}
	c.Certificate = append([][]byte(nil), c.Certificate...)
	for i := range c.Certificate {
		c.Certificate[i] = bytes.Clone(c.Certificate[i])
	}
	c.OCSPStaple = bytes.Clone(c.OCSPStaple)
	c.SignedCertificateTimestamps = append([][]byte(nil), c.SignedCertificateTimestamps...)
	for i := range c.SignedCertificateTimestamps {
		c.SignedCertificateTimestamps[i] = bytes.Clone(c.SignedCertificateTimestamps[i])
	}
	if len(c.Certificate) > 0 {
		c.Leaf, _ = x509.ParseCertificate(c.Certificate[0])
	}
	return c
}
func (c *BootstrapCredentials) Binding() BootstrapBinding {
	if c == nil {
		return BootstrapBinding{}
	}
	return cloneBinding(c.binding)
}
func (c *BootstrapCredentials) Role() BootstrapRole {
	if c == nil {
		return ""
	}
	return c.role
}
func (c *BootstrapCredentials) TLSConfig() *tls.Config {
	if c == nil || c.tls == nil {
		return nil
	}
	config := c.tls.Clone()
	config.Certificates = append([]tls.Certificate(nil), config.Certificates...)
	for i := range config.Certificates {
		config.Certificates[i] = cloneCertificate(config.Certificates[i])
	}
	if config.RootCAs != nil {
		config.RootCAs = config.RootCAs.Clone()
	}
	if config.ClientCAs != nil {
		config.ClientCAs = config.ClientCAs.Clone()
	}
	return config
}
func (c *BootstrapCredentials) Headers() map[string]string {
	headers := map[string]string{}
	if c != nil {
		for key, value := range c.headers {
			headers[key] = value
		}
	}
	return headers
}
func (c *BootstrapCredentials) Authorize(ctx context.Context, values []string) bool {
	if c == nil || ctx == nil || ctx.Err() != nil {
		return false
	}
	allowed := c.authorization == nil && c.binding.Authentication == LocalPrivate || c.authorization != nil && c.authorization(ctx, values)
	return allowed && ctx.Err() == nil
}
func (c *BootstrapCredentials) CheckReference(ref ServiceRef) error {
	if c == nil {
		return bootstrapError()
	}
	expected, err := c.binding.ServiceRef(ref.InstanceID)
	if err != nil || expected != ref {
		return bootstrapError()
	}
	return nil
}
