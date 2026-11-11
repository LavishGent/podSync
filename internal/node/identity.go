// Package node derives the identity a pod uses in versions and logs.
package node

import (
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"time"
)

// hostname is a variable so tests can exercise the fallback path.
var hostname = os.Hostname

// NewID returns a node ID of the form "<hostname>-<startup unix nanos>". If the
// hostname is empty or unavailable (outside Kubernetes), it returns 8 random
// bytes as hex. Each call returns a distinct ID.
func NewID() string {
	if h, err := hostname(); err == nil && h != "" {
		return h + "-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("podsync: crypto/rand unavailable: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
