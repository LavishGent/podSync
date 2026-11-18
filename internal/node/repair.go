package node

import (
	"context"
	"math/rand/v2"
	"time"
)

// repairLoop runs until Close. The first tick fires after FirstRepairDelay
// (Addendum A2), later ticks after RepairInterval jittered by +-10%.
func (n *Node[K, V]) repairLoop() {
	delay := n.cfg.FirstRepairDelay
	for {
		t := time.NewTimer(delay)
		select {
		case <-n.rootCtx.Done():
			t.Stop()
			return
		case <-t.C:
		}
		n.repairTick(n.rootCtx)
		delay = time.Duration(float64(n.cfg.RepairInterval) * (0.9 + 0.2*rand.Float64()))
	}
}

// repairTick pulls one random peer's state through the paged state exchange
// and merges it. When this node holds fewer than MaxStateEntries/4 entries it
// also pushes its own entries, shard by shard, in the same requests. A failed
// page ends the round and is counted; the next tick picks a peer again.
func (n *Node[K, V]) repairTick(ctx context.Context) {
	if !n.Ready() {
		return
	}
	addrs := n.peers.Peers()
	if len(addrs) == 0 {
		return
	}
	addr := addrs[rand.IntN(len(addrs))]
	push := n.store.Stats().TotalEntries < n.cfg.MaxStateEntries/4
	received, applied := 0, 0
	err := n.pullFrom(ctx, addr, &hydrationProgress{}, &received, &applied, push)
	n.repaired.Add(uint64(applied))
	n.warnEnvelope(received)
	if err != nil {
		if ctx.Err() == nil {
			n.repairFailures.Add(1)
			n.log.Warn("repair failed", "peer", addr, "err", err)
		}
		return
	}
	if applied > 0 {
		n.log.Info("repaired", "peer", addr, "applied", applied)
	}
}
