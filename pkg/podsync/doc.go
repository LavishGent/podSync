// Package podsync replicates small, short-lived, pod-originated state between
// the pods of one Kubernetes Deployment, from memory to memory, with no
// external cache service.
//
// Reads are local. Writes apply locally and replicate to every peer
// asynchronously; concurrent writes to one key resolve last-writer-wins.
//
// Example:
//
//	cache, err := podsync.New(podsync.Config[string, Flag]{
//		Resolver: podsync.DNS("podsync-headless", "default", 9090),
//		Codec:    podsync.JSONCodec[Flag]{},
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	if err := cache.Start(ctx); err != nil { // returns once hydrated
//		log.Fatal(err)
//	}
//	defer cache.Close(shutdownCtx)
//
//	cache.Set(ctx, "checkout-v2", Flag{On: true}, podsync.WithTTL(10*time.Minute))
//	flag, ok := cache.Get("checkout-v2") // local, no network
//
// # Operating envelope
//
// Designed for 3 to 25 pods and roughly 50k entries (Config.MaxStateEntries) of
// small values. Hydration and repair move full state, one shard page at a time,
// so cost grows with entry count. Keys are strings or integers. TTLs should be
// at least seconds long, because each pod compares expiry against its own
// clock. An entry written by a pod that has since left lives on the others
// until its TTL, so always set one for per-pod state. Not for durable data,
// queues, or anything needing strong consistency.
package podsync
