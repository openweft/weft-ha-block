# weft-ha-block

[![License: BSD-3-Clause](https://img.shields.io/badge/license-BSD--3--Clause-blue)](LICENSE)
[![Go](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](https://go.dev/dl/)
[![CGO_ENABLED=0](https://img.shields.io/badge/CGO__ENABLED-0-success)](https://pkg.go.dev/cmd/cgo)

The **weft binding** for [go-volumes](https://github.com/go-volumes)
replicated-volume high availability. It is the integrator that plugs weft's
substrate into the two go-volumes HA seams so that **exactly one node is the
active writer** for a replicated block volume:

- an **etcd `Coordinator`** (`internal/etcd`) implementing
  [`replicaha.Coordinator`](https://github.com/go-volumes/replica-ha) over etcd
  `concurrency.Election` — lease-based leader election + lease-bound membership;
- a **weft STONITH `Fencer`** (`internal/fencing`) implementing
  [`replica.Fencer`](https://github.com/go-volumes/replica) by hard-stopping the
  prior writer's micro-VM through the weft agent and confirming the stop;
- a per-node **agent** (`cmd/weft-ha-block`) that wires the NBD replicas, the
  `replica.Engine`, the `Coordinator`, the `Fencer` and the
  `replicaha.Controller` together and serves the gated single-active-writer
  device over NBD.

This repo **imports** `go-volumes/{replica,replica-ha,nbd}`; it does not modify
them. The safety-critical state machine lives in `go-volumes/replica-ha`; this
repo only supplies the etcd + weft implementations of its seams.

## Pipeline

```
NBD clients (remote pool volumes over WireGuard)   github.com/go-volumes/nbd
   └─▶ replica.Engine  (synchronous N-way mirror)  github.com/go-volumes/replica
        └─▶ replicaha.Controller  (fence-before-promote)  github.com/go-volumes/replica-ha
              ├─ Coordinator: etcd lease election      internal/etcd     (this repo)
              └─ Fencer:      weft StONITH (StopVM)     internal/fencing  (this repo)
        └─▶ ActiveDevice (write gate) served over NBD on a local / WireGuard addr
```

## The two seams

### `internal/etcd` — `Coordinator`

Leader election is a `concurrency.Election` keyed under
`/weft-ha-block/<cluster>/leader`; the election value is the node ID. Membership
is a key under `/weft-ha-block/<cluster>/members/` written with the **same
session lease**, so a fenced or partitioned node drops out of `Members`
automatically when its lease expires.

`Observe` maps etcd's `election.Observe` stream onto `replicaha.Leadership`:

| etcd event | `Leadership` |
| --- | --- |
| leader value `val` | `{Leader: val, IsSelf: val==NodeID, Term: <leader key CreateRevision>}` |
| `session.Done()` (lease lost) | `{IsSelf: false}` then **channel closed** |
| observe stream closed | `{IsSelf: false}` then **channel closed** |

`Term` is the leader key's `CreateRevision`: strictly greater on every new
leader, so it is a monotonically non-decreasing fencing token / epoch. The
lease-loss path is the safety hinge — the `Controller` demotes and stops writing
on the `IsSelf:false` observation (and again on the channel close).

### `internal/fencing` — `VMFencer`

`Fence(ctx, writer)` resolves the writer's node ID to a weft VM name, issues a
**hard** `StopVM` (not a graceful shutdown), then `WaitStopped` polls `VMStatus`
until a confirmed-stopped state. It returns nil **only** after confirmation; a
`WaitStopped` timeout / cancel / failure returns a wrapped `ErrFenceConfirmation`
so the `Controller` stays passive and refuses to promote. Fencing an
already-stopped / absent VM is idempotent (returns nil). `grpc_stopper.go` is the
concrete `VMStopper` over `weft-proto` + gRPC with secure-by-default TLS.

## Run

```
weft-ha-block agent \
  --node-name node-a --cluster-name vol-1 \
  --etcd https://etcd-1:2379,https://etcd-2:2379 \
  --replica r-a=10.10.0.1:10809/vol-1 --replica r-b=10.10.0.2:10809/vol-1 \
  --local-replica r-a --min-in-sync 1 \
  --serve 10.10.0.1:10810 --export vol-1 \
  --weft-agent weft-agent.dc1:9090 --weft-project storage \
  --weft-tls-ca /etc/weft/ca.pem
```

The served NBD device returns `ErrNotLeader` on writes until this node is the
confirmed leader; a local filesystem driver or VM image consumes it unchanged.

## Tests

- `internal/fencing`: a fake `VMStopper` covers `Fence` success / StopVM-error /
  WaitStopped-timeout (blocks promotion) / idempotent already-stopped / name
  mapping, plus an in-process gRPC stub server exercising the concrete
  `GRPCStopper`.
- `internal/etcd`: embedded etcd integration — Campaign→IsSelf, Resign release, a
  second node sees the leader, lease-loss surfaces a demotion, `Members` lists
  live peers, **and** a full `replicaha.Controller` + this `Coordinator` + a fake
  `Fencer` + in-memory `replica.Engine` round-trip (become leader → fence →
  `ActiveDevice` writable; lose lease → writes rejected with `ErrNotLeader`).

CI runs `gofmt`/`vet`/`go test -race` natively and cross-builds the whole module
on the 6 supported 64-bit architectures (amd64, arm64, riscv64, loong64,
ppc64le, s390x).

## License

BSD-3-Clause © the openweft/weft-ha-block authors.
