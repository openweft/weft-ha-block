package fencing

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	weftv1 "github.com/openweft/weft-proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// stubAgent is an in-process WeftAgent server. It returns a programmable VM
// state for VMStatus and a programmable error for StopVM, so the concrete
// GRPCStopper can be exercised over a real gRPC connection without a weft-agent.
type stubAgent struct {
	weftv1.UnimplementedWeftAgentServer

	stopErr   error
	stopCalls int32

	state    weftv1.VMState
	notFound bool // VMStatus returns codes.NotFound
}

func (s *stubAgent) StopVM(_ context.Context, _ *weftv1.StopVMRequest) (*weftv1.StopVMResponse, error) {
	atomic.AddInt32(&s.stopCalls, 1)
	if s.stopErr != nil {
		return nil, s.stopErr
	}
	return &weftv1.StopVMResponse{}, nil
}

func (s *stubAgent) VMStatus(_ context.Context, req *weftv1.VMStatusRequest) (*weftv1.VMStatusResponse, error) {
	if s.notFound {
		return nil, status.Error(codes.NotFound, "no such vm")
	}
	return &weftv1.VMStatusResponse{Vm: &weftv1.VMInfo{Name: req.GetName(), State: s.state}}, nil
}

// startStubAgent serves stub on a loopback listener and returns its address.
func startStubAgent(t *testing.T, stub *stubAgent) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	weftv1.RegisterWeftAgentServer(srv, stub)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return ln.Addr().String()
}

// TestGRPCStopper_StopAndWait_Confirmed: a healthy stop + a STOPPED status
// round-trips to nil; the pooled conn is reused (one dial), and Close releases it.
func TestGRPCStopper_StopAndWait_Confirmed(t *testing.T) {
	stub := &stubAgent{state: weftv1.VMState_VM_STATE_STOPPED}
	addr := startStubAgent(t, stub)

	s := NewGRPCStopper(addr, "proj", quietLog())
	t.Cleanup(func() { _ = s.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.StopVM(ctx, "vm-1"); err != nil {
		t.Fatalf("StopVM: %v", err)
	}
	if err := s.WaitStopped(ctx, "vm-1", 2*time.Second); err != nil {
		t.Fatalf("WaitStopped: %v", err)
	}
	if got := atomic.LoadInt32(&stub.stopCalls); got != 1 {
		t.Errorf("StopVM server calls = %d; want 1", got)
	}
	if err := s.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}

// TestGRPCStopper_StopVM_NotFound collapses to nil (already torn down).
func TestGRPCStopper_StopVM_NotFound(t *testing.T) {
	stub := &stubAgent{stopErr: status.Error(codes.NotFound, "gone"), state: weftv1.VMState_VM_STATE_STOPPED}
	addr := startStubAgent(t, stub)
	s := NewGRPCStopper(addr, "proj", quietLog())
	t.Cleanup(func() { _ = s.Close() })
	if err := s.StopVM(context.Background(), "vm-x"); err != nil {
		t.Errorf("StopVM NotFound should collapse to nil; got %v", err)
	}
}

// TestGRPCStopper_StopVM_OtherError propagates.
func TestGRPCStopper_StopVM_OtherError(t *testing.T) {
	stub := &stubAgent{stopErr: status.Error(codes.Internal, "boom"), state: weftv1.VMState_VM_STATE_STOPPED}
	addr := startStubAgent(t, stub)
	s := NewGRPCStopper(addr, "proj", quietLog())
	t.Cleanup(func() { _ = s.Close() })
	if err := s.StopVM(context.Background(), "vm-x"); err == nil {
		t.Error("StopVM Internal error should propagate")
	}
}

// TestGRPCStopper_WaitStopped_NotFound counts as stopped.
func TestGRPCStopper_WaitStopped_NotFound(t *testing.T) {
	stub := &stubAgent{notFound: true}
	addr := startStubAgent(t, stub)
	s := NewGRPCStopper(addr, "proj", quietLog())
	t.Cleanup(func() { _ = s.Close() })
	if err := s.WaitStopped(context.Background(), "vm-x", 2*time.Second); err != nil {
		t.Errorf("WaitStopped NotFound should be nil (stopped); got %v", err)
	}
}

// TestGRPCStopper_WaitStopped_Timeout: a never-stopping VM times out with an
// error — the signal that blocks promotion.
func TestGRPCStopper_WaitStopped_Timeout(t *testing.T) {
	stub := &stubAgent{state: weftv1.VMState_VM_STATE_RUNNING}
	addr := startStubAgent(t, stub)
	s := NewGRPCStopper(addr, "proj", quietLog())
	t.Cleanup(func() { _ = s.Close() })
	err := s.WaitStopped(context.Background(), "vm-x", 600*time.Millisecond)
	if err == nil {
		t.Fatal("WaitStopped against a running VM must time out with an error")
	}
}

// TestGRPCStopper_WaitStopped_CtxCancel returns the ctx error.
func TestGRPCStopper_WaitStopped_CtxCancel(t *testing.T) {
	stub := &stubAgent{state: weftv1.VMState_VM_STATE_RUNNING}
	addr := startStubAgent(t, stub)
	s := NewGRPCStopper(addr, "proj", quietLog())
	t.Cleanup(func() { _ = s.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := s.WaitStopped(ctx, "vm-x", 10*time.Second); err == nil {
		t.Fatal("WaitStopped should return when ctx is cancelled")
	}
}

// TestVMFencer_OverGRPC: the full Fence path through the concrete GRPCStopper.
func TestVMFencer_OverGRPC(t *testing.T) {
	stub := &stubAgent{state: weftv1.VMState_VM_STATE_STOPPED}
	addr := startStubAgent(t, stub)
	s := NewGRPCStopper(addr, "proj", quietLog())
	t.Cleanup(func() { _ = s.Close() })

	f := NewVMFencer(s, nil, 2*time.Second, quietLog())
	if err := f.Fence(context.Background(), "writer-1"); err != nil {
		t.Fatalf("Fence over gRPC: %v", err)
	}
}
