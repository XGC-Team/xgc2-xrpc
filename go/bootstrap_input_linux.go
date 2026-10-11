package xrpc

import (
	"bytes"
	"encoding/json"
	"errors"
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

// openPrivateDirectory pins an owned mode0700 directory without following any
// path symlink.
func openPrivateDirectory(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("xrpc: canonical absolute private directory required")
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			continue
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		unix.Close(fd)
		if err != nil {
			return nil, err
		}
		fd = next
	}
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		unix.Close(fd)
		return nil, err
	}
	if stat.Uid != uint32(os.Geteuid()) || stat.Mode&0777 != 0700 {
		unix.Close(fd)
		return nil, errors.New("xrpc: private directory must be owned mode0700")
	}
	return os.NewFile(uintptr(fd), path), nil
}

// readPrivateBootstrapFile pins a regular, single-link, owned mode0600 file
// below an owned mode0700 final parent. It never follows path symlinks, opens
// devices/FIFOs for blocking reads, or looks up an implicit credential path.
// maxBytes must be 1..128KiB; returned material belongs to the caller.
func readPrivateBootstrapFile(path string, maxBytes int) ([]byte, error) {
	if maxBytes < 1 || maxBytes > 128<<10 || len(path) > 4096 || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, 0) || path == "/" {
		return nil, bootstrapError()
	}
	directory, err := openPrivateDirectory(filepath.Dir(path))
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

func loadGrant(data json.RawMessage) (credentialGrant, error) {
	var d grantDescriptor
	if err := strictJSON(data, &d); err != nil {
		return credentialGrant{}, err
	}
	switch d.Kind {
	case "tls_identity":
		if !requiredJSON(data, "kind", "cert_file", "key_file") || d.CAFile != "" || d.TokenFile != "" {
			return credentialGrant{}, bootstrapError()
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		if len(fields) != 3 {
			return credentialGrant{}, bootstrapError()
		}
		cert, err := readPrivateBootstrapFile(d.CertFile, 128<<10)
		if err != nil {
			return credentialGrant{}, err
		}
		key, err := readPrivateBootstrapFile(d.KeyFile, 64<<10)
		if err != nil {
			return credentialGrant{}, err
		}
		defer clear(key)
		return newTLSIdentityGrant(cert, key)
	case "tls_trust":
		if !requiredJSON(data, "kind", "ca_file") {
			return credentialGrant{}, bootstrapError()
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		if len(fields) != 2 {
			return credentialGrant{}, bootstrapError()
		}
		ca, err := readPrivateBootstrapFile(d.CAFile, 128<<10)
		if err != nil {
			return credentialGrant{}, err
		}
		return newTLSTrustGrant(ca)
	case "bearer":
		if !requiredJSON(data, "kind", "token_file") {
			return credentialGrant{}, bootstrapError()
		}
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(data, &fields)
		if len(fields) != 2 {
			return credentialGrant{}, bootstrapError()
		}
		token, err := readPrivateBootstrapFile(d.TokenFile, 1024)
		if err != nil {
			return credentialGrant{}, err
		}
		defer clear(token)
		return newBearerGrant(string(token))
	default:
		return credentialGrant{}, bootstrapError()
	}
}

// LoadBootstrapInput consumes exactly the explicit owner argument's file. It
// loads and validates every named grant once before returning usable native
// credentials. The owner still reserves/listens/readies/drains its own host.
func LoadBootstrapInput(path string, role BootstrapRole) (*BootstrapInput, error) {
	if role != BootstrapServer && role != BootstrapClient {
		return nil, bootstrapError()
	}
	data, err := readPrivateBootstrapFile(path, MaxBootstrapBytes)
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
	binding, err := parseBootstrapBinding(document.Binding)
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
	grants := make(map[string]credentialGrant, len(document.Grants))
	for handle, descriptor := range document.Grants {
		if !ValidID(handle) {
			return nil, bootstrapError()
		}
		value, err := loadGrant(descriptor)
		if err != nil {
			return nil, err
		}
		grants[handle] = value
	}
	credentials, err := binding.resolveCredentials(func(handle string) (credentialGrant, error) {
		value, ok := grants[handle]
		if !ok {
			return credentialGrant{}, bootstrapError()
		}
		return value, nil
	}, role)
	if err != nil {
		return nil, err
	}
	return &BootstrapInput{binding: binding, application: bytes.Clone(document.Application), credentials: credentials}, nil
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
