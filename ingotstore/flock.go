package ingotstore

import (
	"fmt"
	"os"
	"syscall"
)

// acquireLock takes an advisory flock on the given path. Returns an error
// if the lock is already held by another process.
func acquireLock(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("ingotstore: open lock file: %w", err)
	}

	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("ingotstore: data directory is locked by another process")
	}
	return f, nil
}

// releaseLock releases the advisory flock and closes the file.
func releaseLock(f *os.File) error {
	if f == nil {
		return nil
	}
	// Closing the fd releases the flock.
	return f.Close()
}
