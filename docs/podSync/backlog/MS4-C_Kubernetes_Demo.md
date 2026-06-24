# MS4-C: Kubernetes Orchestration & Verification

## Overview
Prepare deployment manifests, headless service configurations, and HTTP wrapper endpoints for running PodSync on Kubernetes clusters. Deploy a test cluster locally using Kind (Kubernetes in Docker) to verify peer discovery, hydration, scaling up to 3 replicas, and clean scale-down peer eviction.

- **Milestone**: 4 (Consistency & Deploy)
- **Track**: C (Developer C)
- **Status**: Ready
- **Dependencies**: MS2-B (K8s DNS discovery), MS4-B (Public API assembly)

## Phase Reference
This ticket implements:
- [Phase 13 — Kubernetes Demo](file:///c:/Obsidian-Vaults/podSync/PodSync%20-%20Granular%20Build%20Steps.md#L329-L342) (Steps 190–199)

## Detailed Steps

### Kubernetes Manifests
1. Create `deployments/kubernetes/headless-service.yaml`:
   - Set up a headless `Service` (with `clusterIP: None`) targeting PodSync pods, permitting peer discovery via DNS SRV/A records.
2. Create `deployments/kubernetes/deployment.yaml`:
   - Set up a replica set deployment.
   - Configure a readiness probe targeting `/readyz` (which links to the cache's readiness status).

### Demo Test Server
3. Create `examples/k8s-kind/main.go`:
   - Write a simple Go HTTP server wrapping a `Cache` instance.
   - Implement HTTP endpoints:
     - `/set?key=X&val=Y` to write keys.
     - `/get?key=X` to read keys.
     - `/readyz` returning `200 OK` if `Cache.Ready()` is true, and `503 Service Unavailable` otherwise.
4. Create `examples/k8s-kind/Dockerfile` to package this test server.

### Local Kind Cluster Verification
5. Write shell/Makefile scripts to orchestrate local verification:
   - Run `kind create cluster`.
   - Build docker container and load it into Kind: `kind load docker-image`.
   - Apply manifests: `kubectl apply -f deployments/kubernetes/`.
   - Check container logs to verify DNS resolution and successful cluster joins.
   - Verify scaling: `kubectl scale deployment podsync-example --replicas=3`. Verify new replicas hydrate from existing peers and transition to Ready.
   - Test replication: Set a key on pod-1, read it from pod-3.
   - Scale down: Verify that killed pods are marked as Suspect -> Dead and eventually evict from membership lists.
6. Write `docs/kubernetes.md` describing these verification steps in detail.

## Verification & Testing
1. Execute the Kind cluster script locally or in a staging environment. Verify that the headless DNS resolves, pods bootstrap, and keys replicate across pods.
