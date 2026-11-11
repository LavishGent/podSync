// Package node derives the identity a pod uses in versions and logs.
package node

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

// lastSuffix is the most recent hostname-path suffix handed out. It makes the
// suffix strictly increasing within a process even when the clock repeats or
// steps backwards.
var lastSuffix atomic.Int64

func nextSuffix() int64 {
	for {
		last := lastSuffix.Load()
		next := time.Now().UnixNano()
		if next <= last {
			next = last + 1
		}
		if lastSuffix.CompareAndSwap(last, next) {
			return next
		}
	}
}

// hostname is a variable so tests can exercise the fallback path.
var hostname = os.Hostname

// NewID returns a node ID of the form "<hostname>-<startup unix nanos>". If the
// hostname is empty or unavailable (outside Kubernetes), it returns 8 random
// bytes as hex. The suffix is strictly increasing within a process, so calls
// never return the same ID.
func NewID() string {
	if h, err := hostname(); err == nil && h != "" {
		return h + "-" + strconv.FormatInt(nextSuffix(), 10)
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("podsync: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
