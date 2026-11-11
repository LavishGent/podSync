# PodSync Backlog Index

> 📍 [[../rescope_v0.1|Rescope]] · [[../roadmap_v1|Roadmap]] · [[kanban_board|Kanban Board]]

Six sequential tickets deliver v0.1. Each is shippable on its own and depends only on the one before it. The reasoning behind this plan is in [[../rescope_v0.1|rescope_v0.1.md]].

| Ticket | Delivers | Replaces |
|---|---|---|
| [[T1_Versioning_Merge\|T1: Versioning & Merge Rule]] | Lamport clock, single `merge` path, tombstone GC, node ID | MS1-B |
| [[T2_Wire_Encoding\|T2: Wire Encoding]] | gob envelope, length-prefixed frames, `Codec[V]` | MS1-C |
| [[T3_Transport_Peers\|T3: Transport & Peer Resolution]] | Persistent-connection TCP, in-memory transport, DNS resolver | MS2-A, MS2-B, MS3-B |
| [[T4_Replication_Hydration\|T4: Replication & Hydration]] | Direct fanout, receive path, lifecycle, `Ready()` | MS2-C, MS3-A, MS3-C |
| [[T5_Repair\|T5: Repair]] | Periodic full-state exchange with a random peer | MS4-A |
| [[T6_Public_API_Demo_Docs\|T6: Public API, Kind Demo & Docs]] | `pkg/podsync`, rate-limiter demo, README rewrite | MS4-B, MS4-C, MS5-C |

Done before the rescope: [[archive/MS1-A_Repo_Storage|MS1-A]] (storage engine and clock package). Its code is unchanged.

## Status (updated 2026-11-10)

The v0.1 work is a stack of draft PRs. Each PR targets its parent branch. Nothing is merged yet.

| Step | PR | Base | State |
|---|---|---|---|
| Docs addendum, CI `go-version-file`, banners | [#7](https://github.com/LavishGent/podSync/pull/7) | `main` | Draft |
| T1 versioning and merge | [#8](https://github.com/LavishGent/podSync/pull/8) | `docs/v0.1-addendum` | Draft |
| T2 wire encoding | [#9](https://github.com/LavishGent/podSync/pull/9) | `feat/T1-versioning-merge` | Draft, reviewed (3 rounds) |
| T3 transport and peers | [#10](https://github.com/LavishGent/podSync/pull/10) | `feat/T2-wire-encoding` | Draft, reviewed (1 round, 2 blockers fixed) |
| T4 replication and hydration | not opened | `feat/T3-transport-peers` | In review (branch `feat/T4-replication-hydration`) |
| T5 repair | not started | T4 branch | |
| T6 public API, Kind demo, docs | not started | T5 branch | |

[[../rescope_v0.1|PR #6]] (the rescope docs) is a separate, older PR. The stack above is based on `docs/v0.1-addendum`, which contains that content.

### How to resume

1. Start T4 on a new branch `feat/T4-replication-hydration` from `feat/T3-transport-peers`. Stack each later ticket the same way.
2. Write tests first. Run `go vet ./...`, `gofmt -l .` and `go test -race -count=1 ./...` before opening the PR.
3. Open each PR as a draft with `--base <parent-branch>`. Put the command output summary in the PR description.
4. Review rules that worked: run one review round per PR. Block only on correctness bugs against the ticket or the addendum (data loss, protocol mismatch, crash, race, deadlock). Put non-blocking items and nits in the PR description as follow-ups. Do not harden against hostile peers; the optional HMAC handshake and the README security note cover that.
5. The carry-forward items below are non-blocking follow-ups from T2 and T3 review. Take them into the ticket that touches that code.

### Carry-forward items

- **T4 or T6:** cap the shard count at `wire.MaxShards` in config validation.
- **T4:** state messages use `wire.NewStateRequest`, `wire.NewStateResponse` and `DecodeEntries`, not an `Entries` field. Entry count per page is capped at `MaxFrameSize/144`.
- **T4 or T5:** call `transport.Close()` only after the fanout queues drain. `Send` after `Close` returns `transport: closed`.
- **T4 or T5:** `transport.Memory` has no "delivered but response lost" fault. Add one if a repair test needs it.
- **T4 or T6:** `peers.Set` drops resolve errors silently and `SelfAddr` does not log. Log self detection at startup at the call site.
- **T3 follow-up:** a non-cancel `Accept` error in `TCP.Listen` stops accepting silently. Surface it or retry with backoff.
- **T3 follow-up:** `Memory.Close` is a no-op, while `TCP.Close` blocks later sends. Make `Memory` match.
- **T2 follow-up:** a max-count frame of tiny entries transiently allocates about 1 GB. A 50k-entry page is about 465 MB. Revisit if memory use matters.
- **T2 follow-up:** `FromEntry` should reject `Clock == 0`, as `ToEntry` does. `minEntryWire` could be 143.
- **Repo hygiene:** `docs/go_1.27_upgrade.md` is untracked and stale. Rewrite or delete it. It is not committed.
- **T6:** the rate-limiter demo needs a Kind run, or a documented manual script if Kind is unavailable.

## Workflow

1. One branch per ticket: `feat/T<n>-<short-name>`.
2. Each ticket's "Verification & Testing" section is its definition of done. `just test` and `just race` must pass.
3. When a ticket records measured numbers (T2 bench, T5 snapshot bench), write them into the ticket file in the same PR.
4. Move the card on the [[kanban_board|Kanban Board]] when opening and merging the PR.

## Archive

The original 15-ticket, three-track plan lives in `archive/`. It is kept for the design reasoning in each ticket, not as work to do. [[../steps_breakdown_v1|steps_breakdown_v1.md]] refers to the archived tickets and is likewise historical.
