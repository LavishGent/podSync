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

## Workflow

1. One branch per ticket: `feat/T<n>-<short-name>`.
2. Each ticket's "Verification & Testing" section is its definition of done. `just test` and `just race` must pass.
3. When a ticket records measured numbers (T2 bench, T5 snapshot bench), write them into the ticket file in the same PR.
4. Move the card on the [[kanban_board|Kanban Board]] when opening and merging the PR.

## Archive

The original 15-ticket, three-track plan lives in `archive/`. It is kept for the design reasoning in each ticket, not as work to do. [[../steps_breakdown_v1|steps_breakdown_v1.md]] refers to the archived tickets and is likewise historical.
