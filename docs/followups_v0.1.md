# v0.1 follow-ups

Non-blocking items found during review of the v0.1 stack (PRs #7 to #13). None blocks the POC. Each is a candidate for v0.2 or a small cleanup.

## Untested or unverified
- **Kind demo has never run.** `deployment.yaml`, the Dockerfile and the `justfile` are untested because `kind`, `docker` and `just` were not installed. The `kubectl run --command` fix in `demo-hit` (`d86b365`) rests on the `kubectl` help text only. The local script `scripts/demo-local.sh` is the verified path.
- **Review fixes were not re-reviewed** for T3 (`Send`/`Close`, `Listen` hang), T4 (`Close` during `Start`) and T6 (`--command`), following the one-round rule.
- **T5 had no fail-first test run.** Its tests and code were written together.

## Wire and encoding (T2)
- A max-count frame of tiny entries transiently allocates about 1 GB. A 50k-entry page is about 465 MB.
- `minEntryWire` could be 143 instead of 144. The cap is 0.7% conservative.
- `TestTypeDefCheckIgnoresIDAssignment` cannot fail. Replace it with golden bytes from a different registration order.
- `FromEntry` should reject `Clock == 0`, as `ToEntry` does.
- Cap the shard count at `wire.MaxShards` in config validation.
- Per-entry gob roughly doubles wire size. Revisit a shared encoder per blob.
- Different-Go-version gob compatibility of the type-definition pinning is untested.

## Transport and peers (T3)
- A non-cancel `Accept` error (for example EMFILE) stops accepting silently and `Listen` returns nil.
- `Memory` has no "delivered but response lost" fault.
- `Memory.Close` is a no-op, while TCP returns `transport: closed`.
- `Memory` Listen cleanup deletes the handler even if a later Listen replaced it.
- `peers.Set` drops resolve errors silently, and `SelfAddr` does not log.
- The HMAC handshake is optional. Without it, any peer that reaches the port can write state. Use the NetworkPolicy example.

## Replication and hydration (T4)
- A joiner can hydrate empty pages from a peer that is still starting, then become Ready early. This follows from `publishNotReadyAddresses`, and T5 repair converges it. Options: report responder state in `Pong` and prefer Ready peers, or document `maxSurge: 1`.
- A peer that departs during the `Close` drain keeps its queue, which can lengthen shutdown.
- Rescope A5 still names `OutboxSize` and a ring buffer. The code uses `QueueSize` and a buffered channel.
- If every peer departs during hydration, the node waits out `HydrationTimeout`.
- The TCP smoke test picks ports by listen-then-close and could flake.

## Repair (T5)
- `Repaired` counts pull-side applies only.
- The partition test does not isolate fanout from repair.
- The first-delay test bound is loose.
- A bad push blob fails the whole pull page.

## API and demo (T6)
- Duplicate fields in the ticket's Config listing.
- `os.Exit` on SIGTERM during `Start` in the example.
- DNS port is hardcoded to 9090.
- `Secret` is not exposed in the example.
- `Range` scans the full store on each hit.

## Known design limits (documented, not bugs)
- A departed pod's entries live until their TTL.
- Operating envelope is under about 50k entries and 25 pods. Shard digests are v0.2.
