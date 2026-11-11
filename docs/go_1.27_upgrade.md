# Go 1.27 Upgrade

> 📍 [[rescope_v0.1|Rescope]] · [[backlog/Backlog_Index|Backlog Index]] · [[kanban_board|Kanban Board]]

## Overview
How to move PodSync from Go 1.26.3 to Go 1.27 when we choose to. This is not part of v0.1.

- **Status**: Not started
- **Dependencies**: None

## Current state

`go.mod` declares `go 1.26.3` and has no `require` block. CI reads the version from `go.mod` through `actions/setup-go` with `go-version-file: go.mod`, so `go.mod` is the only version pin. There are no Dockerfiles, `go.work`, or `.golangci.yml` pins.

## Steps

1. Bump the directive: `go mod edit -go=1.27.0`. CI picks up the new version from `go.mod` with no workflow change.
2. Verify locally with the new toolchain:
   ```sh
   GOTOOLCHAIN=go1.27.0 go vet ./...
   GOTOOLCHAIN=go1.27.0 go test -race -count=1 ./...
   "$(GOTOOLCHAIN=go1.27.0 go env GOROOT)/bin/gofmt" -l .
   ```
   `GOTOOLCHAIN` only switches the `go` command, so call the toolchain's own `gofmt` by path.
3. Optionally bump the local Go install (mise) so `go version` reports 1.27 without `GOTOOLCHAIN`.

## Open decisions

### Language floor for library consumers

`pkg/podsync` is a public library. `go 1.27.0` makes 1.27 the minimum for anyone importing it; older toolchains must download 1.27. To keep a lower floor while building with 1.27, split the directive:

```
go 1.24

toolchain go1.27.0
```

### GODEBUG defaults

Bumping the `go` line changes `GODEBUG` defaults. If behavior shifts after step 1, check those first, and pin the old behavior with a `godebug` line in `go.mod` rather than reverting the bump.
