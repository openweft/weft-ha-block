// Package fencing implements the go-volumes replica.Fencer seam over weft's
// STONITH: it hard-stops the micro-VM hosting a stale block-volume writer and
// confirms it is stopped before the replicaha.Controller is allowed to promote.
//
// This is a weft-side INTEGRATOR of go-volumes: VMFencer satisfies
// github.com/go-volumes/replica.Fencer; it does not modify go-volumes. It
// mirrors weft-ha-postgresql's internal/fencing (the VMStopper interface +
// VMFencer + grpc_stopper) but fences a replicated-volume WRITER rather than a
// Postgres primary — same STONITH model, different data plane.
package fencing

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-volumes/replica"
)

// Compile-time assertion that *VMFencer satisfies the go-volumes seam.
var _ replica.Fencer = (*VMFencer)(nil)

// ErrFenceConfirmation is returned (wrapped) when Fence could not confirm the
// writer's VM reached a stopped state. The replicaha.Controller treats any
// non-nil Fence error as "do NOT promote" and stays passive, so a wedged VM that
// might come back and double-write can never trigger a split-brain promotion.
var ErrFenceConfirmation = errors.New("fencing: could not confirm vm stopped")

// VMStopper is the minimal weft-agent surface VMFencer needs. It is an interface
// so the fence logic is testable with a fake and so this package's import graph
// stays narrow (the concrete gRPC client in grpc_stopper.go pulls in weft-proto
// + grpc only).
//
// StopVM submits a HARD stop and returns once accepted. WaitStopped blocks until
// the VM reports a confirmed-stopped state or the timeout fires — that timeout
// MUST surface as an error (never nil), because promoting into a maybe-stopped
// writer is the split-brain failure mode.
type VMStopper interface {
	StopVM(ctx context.Context, name string) error
	WaitStopped(ctx context.Context, name string, timeout time.Duration) error
}

// VMNameFunc maps a writer identity (the replicaha NodeID of the stale leader,
// as passed to Fence) to the weft VM name to stop. Identity-by-default; inject a
// custom mapping when the node ID and VM name differ.
type VMNameFunc func(writer string) string

// VMFencer fences a stale block-volume writer by hard-stopping its micro-VM via
// the weft agent and confirming the stop.
//
// Fence-before-promote contract (driven by replicaha.Controller):
//
//  1. The Controller calls Fence(ctx, prevWriter) BEFORE opening its write gate.
//  2. Fence resolves prevWriter -> VM name, issues a HARD StopVM (not a graceful
//     shutdown — a wedged writer is not trusted to honour SIGTERM), then
//     WaitStopped until the VM is provably stopped.
//  3. Fence returns nil ONLY after that confirmation. Any error keeps the new
//     leader passive (replicaha stays RoleFencePending, never writes) and the
//     next observation retries.
type VMFencer struct {
	stopper        VMStopper
	nameFor        VMNameFunc
	confirmTimeout time.Duration
	log            *slog.Logger
}

// NewVMFencer wires a fencer. confirmTimeout defaults to 30s when <= 0 (most
// healthy hypervisors stop a VM in well under 10s; past 30s the host itself is
// usually in trouble and the runbook is a manual failover). nameFor defaults to
// identity (writer == VM name). log nil discards.
func NewVMFencer(stopper VMStopper, nameFor VMNameFunc, confirmTimeout time.Duration, log *slog.Logger) *VMFencer {
	if confirmTimeout <= 0 {
		confirmTimeout = 30 * time.Second
	}
	if nameFor == nil {
		nameFor = func(writer string) string { return writer }
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &VMFencer{stopper: stopper, nameFor: nameFor, confirmTimeout: confirmTimeout, log: log}
}

// Fence isolates writer by hard-stopping its VM and waiting for confirmation.
// It returns nil only once the VM is provably stopped.
//
// Idempotency: a StopVM against an already-absent VM, and a WaitStopped that
// finds the VM already stopped/absent, both succeed — fencing an already-dead
// writer returns nil. A StopVM error does NOT short-circuit: we still demand a
// WaitStopped confirmation, because the writer may be mid-teardown; only a
// WaitStopped failure (timeout / ctx cancel / unstoppable) returns an error and
// blocks promotion.
func (f *VMFencer) Fence(ctx context.Context, writer string) error {
	vm := f.nameFor(writer)
	f.log.Warn("fencing block-volume writer", "writer", writer, "vm", vm)
	if err := f.stopper.StopVM(ctx, vm); err != nil {
		// Don't promote on the strength of a StopVM error alone, but don't bail
		// either: the authoritative signal is WaitStopped. Log and proceed.
		f.log.Warn("StopVM returned", "vm", vm, "err", err)
	}
	if err := f.stopper.WaitStopped(ctx, vm, f.confirmTimeout); err != nil {
		return fmt.Errorf("%w: %w", ErrFenceConfirmation, err)
	}
	f.log.Info("writer fenced (vm confirmed stopped)", "writer", writer, "vm", vm)
	return nil
}
