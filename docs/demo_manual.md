# Demo manual

The demo is a rate limiter (`examples/ratelimit`): `GET /hit?client=<id>` allows 5 hits per 10 s window per client, and the budget is shared by every replica. Each pod counts only its own hits under `limit:<client>:<nodeID>`; a request sums those keys, so you can send hits to different pods and the limit still holds. The sum is eventually consistent and can overshoot a little.

Two ways to run it. **The local run is tested. The Kind run is untested**: it was written on a machine without `kind`, `just` or Docker, so expect to fix small things.

## A. Local, no Kubernetes (tested)

Needs Go and `curl`.

```sh
scripts/demo-local.sh        # or: just demo-local
```

It builds the example, starts three instances on `127.0.0.1` (HTTP 18081-18083, replication 19091-19093, `PEERS` as a static list), waits for `/readyz`, then sends 12 hits for `alice` round-robin. Expected shape:

```
hit  1 -> pod port 18081  X-Pod=<host>-<id1>  HTTP 200
hit  2 -> pod port 18082  X-Pod=<host>-<id2>  HTTP 200
...
hit  5 -> pod port 18082  X-Pod=<host>-<id2>  HTTP 200
hit  6 -> pod port 18083  X-Pod=<host>-<id3>  HTTP 429
...
== allowed 5 of 12 (expected 5)
```

Three different `X-Pod` values, exactly 5 `200`s, then `429`s. Then `/stats` JSON from pod 0 (`"State":"Ready"`, `"Peers":2`, `ReceivedMutations` equal to the other pods' hits) and the pod's info logs (`peers changed`, `hydrated`, `ready`). The script sleeps 0.2 s between hits so replication keeps up. Remove that sleep and a burst can overshoot, which is the documented behaviour.

By hand, in three terminals (change `0`/`1`/`2` and ports):

```sh
go build -o /tmp/ratelimit ./examples/ratelimit
PEERS=127.0.0.1:19091,127.0.0.1:19092,127.0.0.1:19093
HTTP_ADDR=127.0.0.1:18081 REPL_ADDR=127.0.0.1:19091 SELF=127.0.0.1:19091 PEERS=$PEERS /tmp/ratelimit
curl -si '127.0.0.1:18081/hit?client=alice'
```

## B. Kind (untested)

Needs `kind`, `kubectl`, Docker, `just`. Each step with the output to expect.

1. **Cluster, image, deploy** (`just demo-up`):
   ```sh
   kind create cluster --name podsync
   docker build -t podsync-ratelimit:dev .
   kind load docker-image podsync-ratelimit:dev --name podsync
   kubectl apply -f deployments/kubernetes/headless-service.yaml -f deployments/kubernetes/deployment.yaml
   kubectl rollout status deployment/podsync-app --timeout=120s
   ```
   Ends with `deployment "podsync-app" successfully rolled out`. `kubectl get pods -l app=podsync-app` shows three pods `1/1 Running`.

2. **Hit it** (`just demo-hit`). A `kubectl port-forward` to a Service pins one pod, so requests come from a pod inside the cluster:
   ```sh
   kubectl run hitter --rm -i --restart=Never --image=curlimages/curl --command -- \
     sh -c 'for i in $(seq 12); do curl -si "http://podsync-app/hit?client=alice" | grep -iE "^(HTTP|x-pod)" | tr "\r\n" "  "; echo; sleep 0.2; done'
   ```
   Expect 12 lines like `HTTP/1.1 200 OK  X-Pod: podsync-app-6d9f...-abcde-<n>`: at least two distinct `X-Pod` values (the Service picks pods at random), about 5 lines with `200` then `429 Too Many Requests`. A little overshoot (6 or 7 allowed) is possible.

3. **Scale up** (`just demo-scale 5`):
   ```sh
   kubectl scale deployment/podsync-app --replicas=5
   kubectl logs -l app=podsync-app --tail=20 --prefix | grep -E 'hydrated|peers changed'
   ```
   The two new pods log `msg=hydrated peer=<ip>:9090 entries=<n>` and then `msg=ready`; the old pods log `peers changed added=[<new ip>:9090]`. Re-run step 2 within the same window: the count includes hits made before the scale-up.

4. **Tear down** (`just demo-down`): `kind delete cluster --name podsync`.

If something fails: `kubectl logs <pod>` (look for `peer resolution failed`), `kubectl get endpoints podsync-headless` (should list all pod IPs, including not-ready ones), and `curl <pod-ip>:8080/stats` from inside the cluster.
