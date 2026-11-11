package node

import (
	"context"
	"fmt"
	"time"

	"github.com/LavishGent/podsync/internal/wire"
)

// handle serves peer requests in every state except Closed.
func (n *Node[K, V]) handle(_ context.Context, _ string, t wire.MsgType, payload []byte) (wire.MsgType, any, error) {
	if State(n.state.Load()) == StateClosed {
		return 0, nil, ErrClosed
	}
	msg, err := wire.DecodeMessage(t, payload)
	if err != nil {
		n.decodeErrors.Add(1)
		return 0, nil, err
	}
	switch m := msg.(type) {
	case wire.Mutation:
		n.recvMutations.Add(1)
		applied, err := n.applyEntry(m.Entry)
		if err != nil {
			return 0, nil, err
		}
		if !applied {
			n.recvConflicts.Add(1)
		}
		return wire.MsgMutationAck, wire.MutationAck{Applied: applied}, nil
	case wire.StateRequest:
		// One page: this node's entries for the requested shard. A shard it
		// does not have yields an empty page (SnapshotShard returns nil).
		kes := n.store.SnapshotShard(m.Shard)
		es := make([]wire.Entry, 0, len(kes))
		for _, ke := range kes {
			w, err := wire.FromEntry(ke.Key, ke.Entry, n.keys, n.vals)
			if err != nil {
				return 0, nil, fmt.Errorf("node: encode shard %d: %w", m.Shard, err)
			}
			es = append(es, w)
		}
		resp, err := wire.NewStateResponse(n.id, m.Shard, n.store.NumShards(), es)
		if err != nil {
			return 0, nil, fmt.Errorf("node: state response for shard %d: %w", m.Shard, err)
		}
		return wire.MsgStateResponse, resp, nil
	case wire.Ping:
		return wire.MsgPong, wire.Pong{Origin: n.id, Clock: n.lamport.Current()}, nil
	}
	return 0, nil, fmt.Errorf("node: unexpected message %s", t)
}

// applyEntry observes the entry's version and merges it. A decode failure is
// counted and returned; local state is untouched.
func (n *Node[K, V]) applyEntry(w wire.Entry) (bool, error) {
	key, e, err := wire.ToEntry(w, n.keys, n.vals)
	if err != nil {
		n.decodeErrors.Add(1)
		return false, err
	}
	n.lamport.Observe(e.Version)
	return n.store.Merge(key, e), nil
}

// hydrate pulls state from peers one shard page at a time (Addendum A8).
// It returns nil when every page is merged or HydrationTimeout expires, and
// ctx's error if Start's ctx ends first.
func (n *Node[K, V]) hydrate(ctx context.Context) error {
	hctx, cancel := context.WithTimeout(ctx, n.cfg.HydrationTimeout)
	defer cancel()
	prog := &hydrationProgress{}
	for {
		addrs := n.peers.Peers()
		n.cfg.shuffle(addrs)
		received := 0
		for _, addr := range addrs {
			err := n.hydrateFrom(hctx, addr, prog, &received)
			if err == nil {
				n.warnEnvelope(received)
				n.log.Info("hydrated", "peer", addr, "entries", received)
				return nil
			}
			n.log.Warn("hydration from peer failed", "peer", addr, "shard", prog.next, "err", err)
			if hctx.Err() != nil {
				break
			}
		}
		n.warnEnvelope(received)
		select {
		case <-hctx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}
			n.log.Error("hydration timed out; going Ready with partial state",
				"timeout", n.cfg.HydrationTimeout, "merged_through_shard", prog.next)
			return nil
		case <-time.After(n.cfg.HydrationRetryInterval):
		}
	}
}

func (n *Node[K, V]) warnEnvelope(received int) {
	if received > n.cfg.MaxStateEntries {
		n.log.Warn("state exceeds the documented operating envelope",
			"entries", received, "max_state_entries", n.cfg.MaxStateEntries)
	}
}

// hydrationProgress remembers where the walk stopped so the next peer can
// resume there, but only if it reports the same shard count.
type hydrationProgress struct {
	next      int // next shard to request
	numShards int // peer shard count seen so far; 0 before the first page
}

func (n *Node[K, V]) hydrateFrom(ctx context.Context, addr string, p *hydrationProgress, received *int) error {
	shard := p.next
	for {
		resp, err := n.requestPage(ctx, addr, shard)
		if err != nil {
			return fmt.Errorf("shard %d: %w", shard, err)
		}
		if p.numShards != 0 && resp.NumShards != p.numShards {
			// Different layout: shard indexes mean different keys. Start over.
			p.next, p.numShards, shard = 0, 0, 0
			continue
		}
		es, err := resp.DecodeEntries()
		if err != nil {
			return fmt.Errorf("shard %d: %w", shard, err)
		}
		for _, w := range es {
			if _, err := n.applyEntry(w); err != nil {
				n.log.Warn("skipping undecodable entry", "peer", addr, "err", err)
			}
		}
		*received += len(es)
		p.numShards = resp.NumShards
		shard++
		p.next = shard
		if shard >= max(n.store.NumShards(), resp.NumShards) {
			return nil
		}
	}
}

func (n *Node[K, V]) requestPage(ctx context.Context, addr string, shard int) (wire.StateResponse, error) {
	req, err := wire.NewStateRequest(n.id, shard, n.store.NumShards(), nil)
	if err != nil {
		return wire.StateResponse{}, err
	}
	rctx, cancel := context.WithTimeout(ctx, n.cfg.RequestTimeout)
	defer cancel()
	rt, payload, err := n.tr.Send(rctx, addr, wire.MsgStateRequest, req)
	if err != nil {
		return wire.StateResponse{}, err
	}
	if rt != wire.MsgStateResponse {
		return wire.StateResponse{}, fmt.Errorf("unexpected reply %s", rt)
	}
	msg, err := wire.DecodeMessage(rt, payload)
	if err != nil {
		return wire.StateResponse{}, err
	}
	return msg.(wire.StateResponse), nil
}
