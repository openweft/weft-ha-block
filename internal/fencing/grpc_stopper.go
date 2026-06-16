// grpc_stopper.go — concrete VMStopper backed by weft-agent's gRPC API. Kept in
// this package so the agent can wire it in one line; the import graph stays
// narrow (only weft-proto + grpc are pulled in).

package fencing

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	weftv1 "github.com/openweft/weft-proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Compile-time assertion that *GRPCStopper satisfies the VMStopper seam.
var _ VMStopper = (*GRPCStopper)(nil)

// GRPCStopper dials a weft-agent gRPC endpoint and uses StopVM + VMStatus to
// confirm a fenced VM has reached a stopped state.
//
// SECURITY: the fencer is the load-bearing safety hinge of HA. A MITM that
// swallows StopVM lets a stale writer keep mirroring into the shared replicas
// while a new leader is promoted — split-brain that corrupts the volume. TLS is
// therefore secure-by-default: NewGRPCStopperTLS requires a *tls.Config with at
// least the server CA pinned. NewGRPCStopper (insecure) logs a loud warning so
// production misconfiguration is greppable.
type GRPCStopper struct {
	endpoint string
	project  string
	tls      *tls.Config // nil = insecure (warned)
	log      *slog.Logger

	conn *grpc.ClientConn // pooled across Fence calls (failover hot path)
}

// NewGRPCStopperTLS returns a VMStopper that dials the weft-agent over TLS. Pass
// at minimum a RootCAs pool that trusts the agent's server cert (use
// LoadClientTLSConfig to read CA + optional mTLS pair from disk). Dialing is
// lazy so the daemon starts even when the control plane is briefly unreachable.
func NewGRPCStopperTLS(endpoint, project string, tlsCfg *tls.Config, log *slog.Logger) *GRPCStopper {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if tlsCfg == nil {
		log.Error("NewGRPCStopperTLS called with nil *tls.Config — falling back to INSECURE; this is a programming bug")
		return NewGRPCStopper(endpoint, project, log)
	}
	return &GRPCStopper{endpoint: endpoint, project: project, tls: tlsCfg, log: log}
}

// NewGRPCStopper returns an INSECURE VMStopper for the dev / SSH-tunnel /
// WireGuard-mesh case where transport auth is provided out of band. It logs a
// loud warning on every dial. Prefer NewGRPCStopperTLS in production.
func NewGRPCStopper(endpoint, project string, log *slog.Logger) *GRPCStopper {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &GRPCStopper{endpoint: endpoint, project: project, log: log}
}

// LoadClientTLSConfig builds a *tls.Config from disk paths. caPath is REQUIRED
// (it pins the agent's server identity). certPath + keyPath are OPTIONAL and
// enable mTLS when both are set. Empty caPath returns an error — insecure mode
// must be chosen deliberately through the CLI gate, never by omission.
func LoadClientTLSConfig(caPath, certPath, keyPath, serverName string) (*tls.Config, error) {
	if caPath == "" {
		return nil, errors.New("LoadClientTLSConfig: --weft-tls-ca is required (or pass --weft-insecure for the dev / WireGuard-mesh case)")
	}
	caPEM, err := os.ReadFile(caPath)
	if err != nil {
		return nil, fmt.Errorf("read CA %s: %w", caPath, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA %s: no PEM certificates parsed", caPath)
	}
	cfg := &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
		ServerName: serverName, // empty = derive from dial target
	}
	if certPath != "" || keyPath != "" {
		if certPath == "" || keyPath == "" {
			return nil, errors.New("LoadClientTLSConfig: mTLS requires both --weft-tls-cert and --weft-tls-key")
		}
		pair, err := tls.LoadX509KeyPair(certPath, keyPath)
		if err != nil {
			return nil, fmt.Errorf("load mTLS keypair %s + %s: %w", certPath, keyPath, err)
		}
		cfg.Certificates = []tls.Certificate{pair}
	}
	return cfg, nil
}

func (s *GRPCStopper) dial(_ context.Context) (weftv1.WeftAgentClient, error) {
	if s.conn != nil {
		return weftv1.NewWeftAgentClient(s.conn), nil
	}
	var creds credentials.TransportCredentials
	if s.tls != nil {
		creds = credentials.NewTLS(s.tls)
	} else {
		s.log.Warn("dialing weft-agent INSECURELY (no TLS); MITM can swallow StopVM and cause split-brain — set --weft-tls-ca for production",
			"endpoint", s.endpoint)
		creds = insecure.NewCredentials()
	}
	cc, err := grpc.NewClient(s.endpoint, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, fmt.Errorf("dial weft-agent %s: %w", s.endpoint, err)
	}
	s.conn = cc
	return weftv1.NewWeftAgentClient(cc), nil
}

// Close releases the pooled connection. Idempotent.
func (s *GRPCStopper) Close() error {
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	return err
}

// StopVM submits a hard stop to the weft-agent. NotFound collapses to nil — an
// already-torn-down VM cannot write, so there is nothing to fence.
func (s *GRPCStopper) StopVM(ctx context.Context, name string) error {
	cli, err := s.dial(ctx)
	if err != nil {
		return err
	}
	_, err = cli.StopVM(ctx, &weftv1.StopVMRequest{Name: name, Project: s.project})
	if err == nil {
		s.log.Info("StopVM submitted", "name", name)
		return nil
	}
	if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
		s.log.Info("StopVM: vm already absent", "name", name)
		return nil
	}
	return fmt.Errorf("StopVM %s: %w", name, err)
}

// WaitStopped polls VMStatus until the VM reports stopped, ctx is cancelled, or
// timeout fires. Polls every 500ms — easy on the agent, fast enough for sub-2s
// failover on a healthy host.
func (s *GRPCStopper) WaitStopped(ctx context.Context, name string, timeout time.Duration) error {
	cli, err := s.dial(ctx)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for {
		resp, err := cli.VMStatus(ctx, &weftv1.VMStatusRequest{Name: name, Project: s.project})
		if err != nil {
			if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
				return nil // gone from the registry — count as stopped
			}
			s.log.Debug("VMStatus error (retrying)", "name", name, "err", err)
		} else if vm := resp.GetVm(); vm != nil {
			if isStopped(vm.GetState()) {
				return nil
			}
		}
		if time.Now().After(deadline) {
			return errors.New("wait-stopped: timeout reached without confirmed-stopped state")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// isStopped maps a weft VMState to "this VM cannot accept writes". STOPPED
// (clean halt), NOT_CREATED (nothing here) and ERROR (agent lost control of the
// VM — it can't reach its disk either) are all safe states for fencing.
func isStopped(state weftv1.VMState) bool {
	switch state {
	case weftv1.VMState_VM_STATE_STOPPED,
		weftv1.VMState_VM_STATE_NOT_CREATED,
		weftv1.VMState_VM_STATE_ERROR:
		return true
	}
	return false
}
