package fencing

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fakeStopper records StopVM/WaitStopped calls and lets a test inject errors.
type fakeStopper struct {
	mu sync.Mutex

	stopErr  error
	waitErr  error
	waitFunc func(ctx context.Context, name string, timeout time.Duration) error

	stopCalls []string
	waitCalls []string
}

func (f *fakeStopper) StopVM(_ context.Context, name string) error {
	f.mu.Lock()
	f.stopCalls = append(f.stopCalls, name)
	err := f.stopErr
	f.mu.Unlock()
	return err
}

func (f *fakeStopper) WaitStopped(ctx context.Context, name string, timeout time.Duration) error {
	f.mu.Lock()
	f.waitCalls = append(f.waitCalls, name)
	wf := f.waitFunc
	err := f.waitErr
	f.mu.Unlock()
	if wf != nil {
		return wf(ctx, name, timeout)
	}
	return err
}

func (f *fakeStopper) stops() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.stopCalls...)
}

func (f *fakeStopper) waits() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.waitCalls...)
}

// TestFence_Success: StopVM ok + WaitStopped ok => nil, both called once.
func TestFence_Success(t *testing.T) {
	fs := &fakeStopper{}
	f := NewVMFencer(fs, nil, time.Second, quietLog())
	if err := f.Fence(context.Background(), "writer-1"); err != nil {
		t.Fatalf("Fence: %v", err)
	}
	if got := fs.stops(); len(got) != 1 || got[0] != "writer-1" {
		t.Errorf("StopVM calls = %v; want [writer-1]", got)
	}
	if got := fs.waits(); len(got) != 1 || got[0] != "writer-1" {
		t.Errorf("WaitStopped calls = %v; want [writer-1]", got)
	}
}

// TestFence_StopVMError_StillConfirms: a StopVM error does NOT short-circuit;
// the authoritative signal is WaitStopped. If WaitStopped confirms, Fence
// succeeds (the writer was mid-teardown).
func TestFence_StopVMError_StillConfirms(t *testing.T) {
	fs := &fakeStopper{stopErr: errors.New("agent busy")}
	f := NewVMFencer(fs, nil, time.Second, quietLog())
	if err := f.Fence(context.Background(), "w"); err != nil {
		t.Fatalf("Fence should succeed when WaitStopped confirms despite StopVM error: %v", err)
	}
	if len(fs.waits()) != 1 {
		t.Errorf("WaitStopped must still be called after a StopVM error")
	}
}

// TestFence_StopVMError_AndWaitFails: StopVM error AND WaitStopped failure =>
// error, blocking promotion.
func TestFence_StopVMError_AndWaitFails(t *testing.T) {
	fs := &fakeStopper{stopErr: errors.New("agent busy"), waitErr: errors.New("still running")}
	f := NewVMFencer(fs, nil, time.Second, quietLog())
	err := f.Fence(context.Background(), "w")
	if err == nil {
		t.Fatal("Fence must error when WaitStopped fails")
	}
	if !errors.Is(err, ErrFenceConfirmation) {
		t.Errorf("error = %v; want wrapped ErrFenceConfirmation", err)
	}
}

// TestFence_WaitTimeout_BlocksPromotion: WaitStopped timeout => wrapped
// ErrFenceConfirmation. The Controller treats this as "do not promote".
func TestFence_WaitTimeout_BlocksPromotion(t *testing.T) {
	fs := &fakeStopper{
		waitFunc: func(ctx context.Context, _ string, timeout time.Duration) error {
			// Simulate a confirm timeout.
			return errors.New("wait-stopped: timeout reached without confirmed-stopped state")
		},
	}
	f := NewVMFencer(fs, nil, 50*time.Millisecond, quietLog())
	err := f.Fence(context.Background(), "w")
	if !errors.Is(err, ErrFenceConfirmation) {
		t.Fatalf("timeout must surface as ErrFenceConfirmation; got %v", err)
	}
}

// TestFence_IdempotentAlreadyStopped: an already-stopped writer (StopVM nil,
// WaitStopped nil) returns nil — fencing a dead writer is a no-op success.
func TestFence_IdempotentAlreadyStopped(t *testing.T) {
	fs := &fakeStopper{} // both nil = already-stopped / absent
	f := NewVMFencer(fs, nil, time.Second, quietLog())
	if err := f.Fence(context.Background(), "ghost"); err != nil {
		t.Fatalf("idempotent fence of an absent writer should return nil: %v", err)
	}
}

// TestFence_VMNameMapping: a custom VMNameFunc remaps writer id -> VM name and
// both StopVM and WaitStopped see the mapped name.
func TestFence_VMNameMapping(t *testing.T) {
	fs := &fakeStopper{}
	nameFor := func(writer string) string {
		if writer == "node-a" {
			return "vm-alpha"
		}
		return writer
	}
	f := NewVMFencer(fs, nameFor, time.Second, quietLog())
	if err := f.Fence(context.Background(), "node-a"); err != nil {
		t.Fatal(err)
	}
	if got := fs.stops(); len(got) != 1 || got[0] != "vm-alpha" {
		t.Errorf("StopVM got %v; want [vm-alpha]", got)
	}
	if got := fs.waits(); len(got) != 1 || got[0] != "vm-alpha" {
		t.Errorf("WaitStopped got %v; want [vm-alpha]", got)
	}
}

// TestNewVMFencer_Defaults: zero/negative timeout defaults to 30s; nil nameFor
// is identity; nil log is accepted.
func TestNewVMFencer_Defaults(t *testing.T) {
	f := NewVMFencer(&fakeStopper{}, nil, 0, nil)
	if f.confirmTimeout != 30*time.Second {
		t.Errorf("confirmTimeout = %v; want 30s", f.confirmTimeout)
	}
	if f.nameFor("x") != "x" {
		t.Errorf("default nameFor must be identity")
	}
	if f.log == nil {
		t.Error("nil log must be replaced with a discard logger")
	}
}

// TestFence_CtxCancel: a cancelled ctx surfacing through WaitStopped blocks
// promotion.
func TestFence_CtxCancel(t *testing.T) {
	fs := &fakeStopper{
		waitFunc: func(ctx context.Context, _ string, _ time.Duration) error {
			return ctx.Err()
		},
	}
	f := NewVMFencer(fs, nil, time.Second, quietLog())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := f.Fence(ctx, "w"); !errors.Is(err, ErrFenceConfirmation) {
		t.Fatalf("cancelled ctx must block promotion via ErrFenceConfirmation; got %v", err)
	}
}
