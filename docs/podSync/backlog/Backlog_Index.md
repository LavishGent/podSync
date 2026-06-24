# PodSync Backlog Index

Welcome to the PodSync Development Backlog. This backlog divides the 223 granular implementation steps into **15 tickets** organized across **5 Milestones**. 

The work is split into three parallel, complementary tracks (Track A, Track B, and Track C) designed for a team of **3 generalist developers**. Each developer should pick up one track per milestone.

## Milestone Roadmap & Ticket Map

| Milestone | Track A (Developer A) | Track B (Developer B) | Track C (Developer C) |
|---|---|---|---|
| **MS1: Foundations & Core Engines** | [[MS1-A_Repo_Storage|MS1-A: Repo & Storage]] | [[MS1-B_Versioning_Identity|MS1-B: Versioning & Identity]] | [[MS1-C_Codecs_Protobuf|MS1-C: Codecs & Protobuf]] |
| **MS2: P2P Transport & Buffers** | [[MS2-A_TCP_Framing_Server|MS2-A: TCP Framing & Server]] | [[MS2-B_Membership_Discovery|MS2-B: Membership & Discovery]] | [[MS2-C_Replication_Buffers|MS2-C: Replication Buffers]] |
| **MS3: Distributed Replication** | [[MS3-A_Replication_Worker|MS3-A: Replication Worker]] | [[MS3-B_SWIM_Probing|MS3-B: SWIM Probing]] | [[MS3-C_Hydration_Lifecycle|MS3-C: Hydration & Lifecycle]] |
| **MS4: Consistency & Deploy** | [[MS4-A_Active_Anti_Entropy|MS4-A: Active Anti-Entropy]] | [[MS4-B_Public_API_Assembly|MS4-B: Public API Assembly]] | [[MS4-C_Kubernetes_Demo|MS4-C: Kubernetes & Demo]] |
| **MS5: Production Readiness** | [[MS5-A_Chaos_Perf_Testing|MS5-A: Chaos & Perf Testing]] | [[MS5-B_Observability_Metrics|MS5-B: Observability & Metrics]] | [[MS5-C_Technical_Docs_DX|MS5-C: Technical Docs & DX]] |

---

## Developer Guide & Workflow

1. **Sprint Planning / Milestone Kickoff**:
   - Align on the current Milestone. All developers must finish their current Milestone tasks before the team proceeds to the next Milestone.
   - Assign Track A, B, and C to Dev A, B, and C.
2. **Implementation**:
   - Create feature branches matching the ticket: `feature/MS[1-5]-[A-C]`.
   - Refer to the build step indices linked in each ticket. Make sure to follow the step-by-step requirements.
3. **Integration & Review**:
   - Ensure all tests written within the ticket pass locally (`go test ./...` and `go test -race ./...`).
   - Merge to `main` upon successful PR review.
