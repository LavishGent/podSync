package version

import "sync/atomic"

// Lamport is a node's logical clock. It is safe for concurrent use.
type Lamport struct {
	nodeID string
	clock  atomic.Uint64
}

// NewLamport returns a clock at zero that stamps versions with nodeID.
func NewLamport(nodeID string) *Lamport {
	return &Lamport{nodeID: nodeID}
}

// NodeID returns the identity stamped on versions from Next.
func (l *Lamport) NodeID() string { return l.nodeID }

// Next advances the clock and returns the version for a new local write.
func (l *Lamport) Next() Version {
	return Version{Clock: l.clock.Add(1), NodeID: l.nodeID}
}

// Observe raises the clock to at least v.Clock. Call it for every received
// version before merging, so the next local write sorts after anything seen.
func (l *Lamport) Observe(v Version) {
	for {
		cur := l.clock.Load()
		if v.Clock <= cur || l.clock.CompareAndSwap(cur, v.Clock) {
			return
		}
	}
}
