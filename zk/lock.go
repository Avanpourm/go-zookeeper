package zk

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrDeadlock is returned by Lock when trying to lock twice without unlocking first
	ErrDeadlock = errors.New("zk: trying to acquire a lock twice")
	// ErrNotLocked is returned by Unlock when trying to release a lock that has not first be acquired.
	ErrNotLocked = errors.New("zk: not locked")
)

// Lock is a mutual exclusion lock.
type Lock struct {
	name     string // data written to the lock node (local non-loopback IP)
	c        *Conn
	path     string
	acl      []ACL
	lockPath string
	seq      int
}

// NewLock creates a new lock instance using the provided connection, path, and acl.
// The path must be a node that is only used by this lock. A lock instances starts
// unlocked until Lock() is called.
func NewLock(c *Conn, path string, acl []ACL) *Lock {
	return &Lock{
		c:    c,
		path: path,
		acl:  acl,
		name: localIP(),
	}
}

// localIP returns the first non-loopback interface address, used to identify
// the lock holder in the lock node data.
func localIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	ip := ""
	for _, addr := range addrs {
		ip = strings.Split(addr.String(), "/")[0]
		if ip != "127.0.0.1" {
			break
		}
	}
	return ip
}

func parseSeq(path string) (int, error) {
	parts := strings.Split(path, "-")
	return strconv.Atoi(parts[len(parts)-1])
}

// Lock attempts to acquire the lock. It will wait to return until the lock
// is acquired or an error occurs. If this instance already has the lock
// then ErrDeadlock is returned.
func (l *Lock) Lock() error {
	if l.lockPath != "" {
		return ErrDeadlock
	}

	prefix := fmt.Sprintf("%s/lock-", l.path)

	path := ""
	var err error
	for i := 0; i < 3; i++ {
		path, err = l.c.CreateProtectedEphemeralSequential(prefix, []byte(l.name), l.acl)
		if err == ErrNoNode {
			// Create parent node.
			parts := strings.Split(l.path, "/")
			pth := ""
			for _, p := range parts[1:] {
				var exists bool
				pth += "/" + p
				exists, _, err = l.c.Exists(pth)
				if err != nil {
					return err
				}
				if exists == true {
					continue
				}
				_, err = l.c.Create(pth, []byte{}, 0, l.acl)
				if err != nil && err != ErrNodeExists {
					return err
				}
			}
		} else if err == nil {
			break
		} else {
			return err
		}
	}
	if err != nil {
		return err
	}

	seq, err := parseSeq(path)
	if err != nil {
		return err
	}

	for {
		children, _, err := l.c.Children(l.path)
		if err != nil {
			return err
		}

		lowestSeq := seq
		prevSeq := -1
		prevSeqPath := ""
		for _, p := range children {
			s, err := parseSeq(p)
			if err != nil {
				return err
			}
			if s < lowestSeq {
				lowestSeq = s
			}
			if s < seq && s > prevSeq {
				prevSeq = s
				prevSeqPath = p
			}
		}

		if seq == lowestSeq {
			// Acquired the lock
			break
		}

		// Wait on the node next in line for the lock
		_, _, ch, err := l.c.GetW(l.path + "/" + prevSeqPath)
		if err != nil && err != ErrNoNode {
			return err
		} else if err != nil && err == ErrNoNode {
			// try again
			continue
		}

		ev := <-ch
		if ev.Err != nil {
			return ev.Err
		}
	}

	l.seq = seq
	l.lockPath = path
	return nil
}

// LockWC acquires the lock like LockW and additionally releases it once a
// value is received from (or close of) c.
func (l *Lock) LockWC(c <-chan bool) (watch <-chan bool, err error) {
	watch, err = l.LockW()
	if err != nil {
		return
	}
	go func() {
		<-c
		l.Unlock()
	}()
	return
}

// LockW acquires the lock and returns a channel that receives false once the
// session is lost or the lock node disappears; the lock is then released.
func (l *Lock) LockW() (watch <-chan bool, err error) {
	if err = l.Lock(); err != nil {
		return
	}
	lockPath := l.lockPath
	chev := make(chan bool)
	watch = chev
	go func() {
		for {
			if l.c.State() != StateHasSession {
				break
			}
			if ok, _, err := l.c.Exists(lockPath); err != nil || !ok {
				break
			}
			time.Sleep(time.Millisecond * 500)
		}
		chev <- false
		time.Sleep(time.Millisecond * 200)
		l.Unlock()
	}()
	return
}

// Unlock releases an acquired lock. If the lock is not currently acquired by
// this Lock instance than ErrNotLocked is returned.
func (l *Lock) Unlock() error {
	if l.lockPath == "" {
		return ErrNotLocked
	}
	if err := l.c.Delete(l.lockPath, -1); err != nil {
		return err
	}
	l.lockPath = ""
	l.seq = 0
	return nil
}
