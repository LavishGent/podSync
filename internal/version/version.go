// Package version provides the Lamport-clock version attached to every entry.
package version

import "strings"

// Version orders writes to a key. Clock is a Lamport timestamp; NodeID breaks
// ties between writes that carry the same Clock.
type Version struct {
	Clock  uint64
	NodeID string
}

// Compare returns -1, 0, or +1 as a is older than, identical to, or newer than b.
// Versions order by Clock, then by NodeID lexicographically. The NodeID
// tie-break is arbitrary but deterministic, so every node picks the same winner.
func Compare(a, b Version) int {
	switch {
	case a.Clock < b.Clock:
		return -1
	case a.Clock > b.Clock:
		return 1
	}
	return strings.Compare(a.NodeID, b.NodeID)
}

// IsZero reports whether v is the zero Version.
func (v Version) IsZero() bool { return v == Version{} }
