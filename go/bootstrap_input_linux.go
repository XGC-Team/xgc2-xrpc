package xrpc

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// BootstrapInput retains one startup credential snapshot. Its metadata getters
// return copies; no domain configuration is interpreted or watched.
type BootstrapInput struct {
	binding     BootstrapBinding
	application json.RawMessage
	grants      map[string]CredentialGrant
	credentials *BootstrapCredentials
}

func (i *BootstrapInput) Binding() BootstrapBinding {
	if i == nil {
		return BootstrapBinding{}
	}
	return cloneBinding(i.binding)
}
func (i *BootstrapInput) Application() json.RawMessage {
	if i == nil {
		return nil
	}
	return bytes.Clone(i.application)
}
func (i *BootstrapInput) Credentials() *BootstrapCredentials {
	if i == nil {
		return nil
	}
	return i.credentials
}
func (i *BootstrapInput) ResolveGrant(handle string) (CredentialGrant, error) {
	if i == nil {
		return CredentialGrant{}, bootstrapError()
	}
	value, ok := i.grants[handle]
	if !ok {
		return CredentialGrant{}, bootstrapError()
	}
	return value, nil
}

// ReadPrivateBootstrapFile pins a regular, single-link, owned mode0600 file
// below an owned mode0700 final parent. It never follows path symlinks, opens
// devices/FIFOs for blocking reads, or looks up an implicit credential path.
// maxBytes must be 1..128KiB; returned material belongs to the caller.
func ReadPrivateBootstrapFile(path string, maxBytes int) ([]byte, error) {
	if maxBytes < 1 || maxBytes > 128<<10 || len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) || path == "/" {
		return nil, bootstrapError()
	}
	directory, err := openLogDirectory(filepath.Dir(path))
	if err != nil {
		return nil, bootstrapError()
	}
	defer directory.Close()
	var parent unix.Stat_t
	if unix.Fstat(int(directory.Fd()), &parent) != nil || parent.Mode&07777 != 0700 {
		return nil, bootstrapError()
	}
	fd, err := unix.Openat(int(directory.Fd()), filepath.Base(path), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, bootstrapError()
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Mode&07777 != 0600 || stat.Uid != uint32(os.Geteuid()) || stat.Nlink != 1 || stat.Size < 1 || stat.Size > int64(maxBytes) {
		return nil, bootstrapError()
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(maxBytes)+1))
	if err != nil || len(data) == 0 || len(data) > maxBytes {
		return nil, bootstrapError()
	}
	return data, nil
}

type grantDescriptor struct {
	Kind      string `json:"kind"`
	CertFile  string `json:"cert_file,omitempty"`
	KeyFile   string `json:"key_file,omitempty"`
	CAFile    string `json:"ca_file,omitempty"`
	TokenFile string `json:"token_file,omitempty"`
}

func loadGrant(data json.RawMessage) (CredentialGrant, error) {
	var d grantDescriptor
	if err := strictJSON(data, &d); err != nil {
		return CredentialGrant{}, err
	}
	switch d.Kind {
	case "tls_identity":
		if !requiredJSON(data, "kind", "cert_file", "key_file") || d.CAFile != "" || d.TokenFile != "" {
			return CredentialGrant{}, bootstrapError()
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		if len(fields) != 3 {
			return CredentialGrant{}, bootstrapError()
		}
		cert, err := ReadPrivateBootstrapFile(d.CertFile, 128<<10)
		if err != nil {
			return CredentialGrant{}, err
		}
		key, err := ReadPrivateBootstrapFile(d.KeyFile, 64<<10)
		if err != nil {
			return CredentialGrant{}, err
		}
		defer clear(key)
		return NewTLSIdentityGrant(cert, key)
	case "tls_trust":
		if !requiredJSON(data, "kind", "ca_file") {
			return CredentialGrant{}, bootstrapError()
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		if len(fields) != 2 {
			return CredentialGrant{}, bootstrapError()
		}
		ca, err := ReadPrivateBootstrapFile(d.CAFile, 128<<10)
		if err != nil {
			return CredentialGrant{}, err
		}
		return NewTLSTrustGrant(ca)
	case "bearer":
		if !requiredJSON(data, "kind", "token_file") {
			return CredentialGrant{}, bootstrapError()
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		if len(fields) != 2 {
			return CredentialGrant{}, bootstrapError()
		}
		token, err := ReadPrivateBootstrapFile(d.TokenFile, 1024)
		if err != nil {
			return CredentialGrant{}, err
		}
		defer clear(token)
		return NewBearerGrant(string(token))
	default:
		return CredentialGrant{}, bootstrapError()
	}
}

// LoadBootstrapInput consumes exactly the explicit owner argument's file. It
// loads and validates every named grant once before returning usable native
// credentials. The owner still reserves/listens/readies/drains its own host.
func LoadBootstrapInput(path string, role BootstrapRole) (*BootstrapInput, error) {
	if role != BootstrapServer && role != BootstrapClient {
		return nil, bootstrapError()
	}
	data, err := ReadPrivateBootstrapFile(path, MaxBootstrapBytes)
	if err != nil {
		return nil, err
	}
	defer clear(data)
	var document struct {
		SchemaVersion int                        `json:"schema_version"`
		Binding       json.RawMessage            `json:"binding"`
		Grants        map[string]json.RawMessage `json:"grants"`
		Application   json.RawMessage            `json:"application,omitempty"`
	}
	if !requiredJSON(data, "schema_version", "binding", "grants") {
		return nil, bootstrapError()
	}
	if err := strictJSON(data, &document); err != nil || document.SchemaVersion != 1 || document.Grants == nil || len(document.Grants) > 32 {
		return nil, bootstrapError()
	}
	binding, err := ParseBootstrapBinding(document.Binding)
	if err != nil {
		return nil, err
	}
	if len(document.Application) > 0 {
		decoder := json.NewDecoder(bytes.NewReader(document.Application))
		decoder.UseNumber()
		if err := scanApplicationJSON(decoder, 0); err != nil {
			return nil, bootstrapError()
		}
	}
	input := &BootstrapInput{binding: binding, application: bytes.Clone(document.Application), grants: make(map[string]CredentialGrant, len(document.Grants))}
	for handle, descriptor := range document.Grants {
		if !ValidID(handle) {
			return nil, bootstrapError()
		}
		value, err := loadGrant(descriptor)
		if err != nil {
			return nil, err
		}
		input.grants[handle] = value
	}
	input.credentials, err = binding.ResolveCredentials(input.ResolveGrant, role)
	if err != nil {
		return nil, err
	}
	return input, nil
}

// The opaque application counts container nesting only, independently of the
// startup wrapper. Scalars and empty root objects are allowed.
func scanApplicationJSON(d *json.Decoder, depth int) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if depth >= 32 {
		return bootstrapError()
	}
	switch delim {
	case '{':
		for d.More() {
			if _, err := d.Token(); err != nil {
				return err
			}
			if err := scanApplicationJSON(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := scanApplicationJSON(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return bootstrapError()
	}
	_, err = d.Token()
	return err
}
