package xrpc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

type FileSinkOptions struct {
	// Directory is an existing explicit private grant, never an SDK-selected
	// HOME/cwd/tmp fallback. Files includes the active file and all archives.
	Directory string
	Name      string
	MaxBytes  int64
	Files     int
}

// RotatingFileSink is a single writer in a pinned private directory. Routine
// diagnostics have no durable experiment-evidence/fsync promise. No external
// rotation may rename this owner's active file concurrently.
type RotatingFileSink struct {
	mu        sync.Mutex
	directory *os.File
	file      *os.File
	lock      *os.File
	options   FileSinkOptions
	size      int64
	closed    bool
}

func openLogDirectory(path string) (*os.File, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("xrpc: canonical absolute log grant required")
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
		return nil, errors.New("xrpc: log grant must be owned mode0700")
	}
	return os.NewFile(uintptr(fd), path), nil
}

func NewRotatingFileSink(options FileSinkOptions) (*RotatingFileSink, error) {
	if options.Name == "" || options.Name == "." || options.Name == ".." || strings.ContainsAny(options.Name, "/\\\x00\r\n") || len(options.Name) > 128 || options.MaxBytes <= 0 || options.Files < 1 || options.Files > 64 {
		return nil, errors.New("xrpc: bounded log name/size/count grant required")
	}
	directory, err := openLogDirectory(options.Directory)
	if err != nil {
		return nil, err
	}
	sink := &RotatingFileSink{directory: directory, options: options}
	lockFD, err := unix.Openat(int(directory.Fd()), options.Name+".xrpc-log.lock", unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0600)
	if err != nil {
		directory.Close()
		return nil, err
	}
	var lockStat unix.Stat_t
	if err = unix.Fstat(lockFD, &lockStat); err != nil || lockStat.Mode&unix.S_IFMT != unix.S_IFREG || lockStat.Nlink != 1 || lockStat.Uid != uint32(os.Geteuid()) || lockStat.Mode&0777 != 0600 {
		unix.Close(lockFD)
		directory.Close()
		return nil, errors.New("xrpc: invalid log writer lock ownership")
	}
	if err = unix.Flock(lockFD, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		unix.Close(lockFD)
		directory.Close()
		return nil, errors.New("xrpc: log sink already has a writer")
	}
	sink.lock = os.NewFile(uintptr(lockFD), options.Name+".xrpc-log.lock")
	for i := 1; i < 64; i++ {
		var stat unix.Stat_t
		name := options.Name + "." + strconv.Itoa(i)
		if err := unix.Fstatat(int(directory.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
			if errors.Is(err, unix.ENOENT) {
				continue
			}
			sink.Close()
			return nil, err
		}
		if i >= options.Files {
			sink.Close()
			return nil, errors.New("xrpc: existing log archives exceed configured file count")
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0777 != 0600 || stat.Size > options.MaxBytes {
			sink.Close()
			return nil, errors.New("xrpc: existing log archive exceeds granted ownership/size")
		}
	}
	if err = sink.openActive(); err != nil {
		sink.Close()
		return nil, err
	}
	return sink, nil
}

func (s *RotatingFileSink) openActive() error {
	fd, err := unix.Openat(int(s.directory.Fd()), s.options.Name, unix.O_WRONLY|unix.O_APPEND|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0600)
	if err != nil {
		return err
	}
	var stat unix.Stat_t
	if err = unix.Fstat(fd, &stat); err != nil {
		unix.Close(fd)
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0777 != 0600 || stat.Size > s.options.MaxBytes {
		unix.Close(fd)
		return errors.New("xrpc: existing log exceeds granted ownership/size")
	}
	s.file = os.NewFile(uintptr(fd), s.options.Name)
	s.size = stat.Size
	return nil
}

func (s *RotatingFileSink) ownedActive() error {
	var path, opened unix.Stat_t
	if err := unix.Fstat(int(s.file.Fd()), &opened); err != nil {
		return err
	}
	if err := unix.Fstatat(int(s.directory.Fd()), s.options.Name, &path, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if path.Dev != opened.Dev || path.Ino != opened.Ino || path.Mode&unix.S_IFMT != unix.S_IFREG || path.Nlink != 1 {
		return errors.New("xrpc: active log namespace was replaced")
	}
	return nil
}

func (s *RotatingFileSink) rotate() error {
	if err := s.ownedActive(); err != nil {
		return err
	}
	dirfd := int(s.directory.Fd())
	last := s.options.Name
	if s.options.Files > 1 {
		last += "." + strconv.Itoa(s.options.Files-1)
	}
	if err := unix.Unlinkat(dirfd, last, 0); err != nil && !errors.Is(err, unix.ENOENT) {
		return err
	}
	for i := s.options.Files - 2; i >= 1; i-- {
		from := s.options.Name + "." + strconv.Itoa(i)
		to := s.options.Name + "." + strconv.Itoa(i+1)
		if err := unix.Renameat(dirfd, from, dirfd, to); err != nil && !errors.Is(err, unix.ENOENT) {
			return err
		}
	}
	if s.options.Files > 1 {
		if err := unix.Renameat(dirfd, s.options.Name, dirfd, s.options.Name+".1"); err != nil {
			return err
		}
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	s.file = nil
	return s.openActive()
}

func (s *RotatingFileSink) Write(body []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.file == nil {
		return 0, os.ErrClosed
	}
	if int64(len(body)) > s.options.MaxBytes {
		return 0, errors.New("xrpc: diagnostic record exceeds file grant")
	}
	if s.size+int64(len(body)) > s.options.MaxBytes {
		if err := s.rotate(); err != nil {
			return 0, fmt.Errorf("xrpc: log rotation failed: %w", err)
		}
	}
	if err := s.ownedActive(); err != nil {
		return 0, err
	}
	n, err := s.file.Write(body)
	s.size += int64(n)
	return n, err
}
func (s *RotatingFileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var err error
	if s.file != nil {
		err = s.file.Close()
	}
	return errors.Join(err, s.lock.Close(), s.directory.Close())
}
