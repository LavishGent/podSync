# PodSync justfile

# Run tests
test:
    go test ./...

# Run tests with race detector
race:
    go test -race ./...

# Run linter
lint:
    golangci-lint run ./...

# Build all packages
build:
    go build ./...

# ---- Demo (UNTESTED: written without kind/docker available; see docs/demo_manual.md) ----

cluster := "podsync"
image := "podsync-ratelimit:dev"

# Run 3 instances locally on 127.0.0.1, no Kubernetes needed (tested)
demo-local:
    scripts/demo-local.sh

# Create a kind cluster, build and load the image, deploy 3 replicas
demo-up:
    kind create cluster --name {{cluster}}
    docker build -t {{image}} .
    kind load docker-image {{image}} --name {{cluster}}
    kubectl apply -f deployments/kubernetes/headless-service.yaml -f deployments/kubernetes/deployment.yaml
    kubectl rollout status deployment/podsync-app --timeout=120s

# Send 12 requests for one client from a pod inside the cluster, through the Service
demo-hit:
    kubectl run hitter --rm -i --restart=Never --image=curlimages/curl -- \
      sh -c 'for i in $(seq 12); do curl -si "http://podsync-app/hit?client=alice" | grep -iE "^(HTTP|x-pod)" | tr "\r\n" "  "; echo; sleep 0.2; done'

# Scale the Deployment to N replicas and follow hydration logs
demo-scale N:
    kubectl scale deployment/podsync-app --replicas={{N}}
    kubectl rollout status deployment/podsync-app --timeout=120s
    kubectl logs -l app=podsync-app --tail=20 --prefix | grep -E 'hydrated|first node|ready|peers changed'

# Delete the kind cluster
demo-down:
    kind delete cluster --name {{cluster}}

# Check that the module has no non-stdlib dependencies
deps:
    go list -deps -f '{{{{if not .Standard}}}}{{{{.ImportPath}}}}{{{{end}}}}' ./... | sort -u
