// Command weft-ha-block is the weft binding for go-volumes replicated-volume
// high availability. One agent runs alongside each consumer of a replicated
// block volume; it elects a leader through etcd, fences the prior writer through
// the weft agent (STONITH), and serves the gated single-active-writer device
// over NBD so a local filesystem / VM can write through it.
//
// Pipeline:
//
//	NBD clients (remote pool volumes)   github.com/go-volumes/nbd
//	      -> replica.Engine             github.com/go-volumes/replica
//	      -> replicaha.Controller       github.com/go-volumes/replica-ha
//	            coordinator: etcd       internal/etcd   (this repo)
//	            fencer:      weft STONITH internal/fencing (this repo)
//	      -> ActiveDevice served over NBD on a local / WireGuard address.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	govnbd "github.com/go-volumes/nbd"
	"github.com/go-volumes/replica"
	replicaha "github.com/go-volumes/replica-ha"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/openweft/weft-ha-block/internal/etcd"
	"github.com/openweft/weft-ha-block/internal/fencing"
)

// Build metadata, injected via -ldflags.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	if err := rootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func rootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:          "weft-ha-block",
		Short:        "weft high-availability binding for go-volumes replicated block volumes",
		Long:         "weft-ha-block elects a leader through etcd, fences the prior writer through\nthe weft agent (STONITH), and serves the gated single-active-writer device\nover NBD so a local consumer can write the replicated volume safely.",
		SilenceUsage: true,
	}
	root.AddCommand(versionCmd(), agentCmd())
	return root
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print version information",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "weft-ha-block %s (commit %s, built %s)\n", version, commit, date)
			return err
		},
	}
}

// replicaSpec is one "name=addr" replica: an NBD endpoint reachable over the
// WireGuard mesh that fronts a remote pool volume.
type config struct {
	nodeName    string
	clusterName string
	etcd        []string
	etcdTTLSec  int

	replicas   []string // "name=host:port[/export]"
	localName  string
	minInSync  int
	nbdTimeout time.Duration

	serveAddr   string // local/WireGuard NBD listen address for the gated device
	exportName  string
	dialTimeout time.Duration

	weftEndpoint  string
	weftProject   string
	weftTLSCA     string
	weftTLSCert   string
	weftTLSKey    string
	weftTLSServer string
	weftInsecure  bool
	fenceTimeout  time.Duration

	// vmMap maps a writer node id to a weft VM name ("nodeid=vmname,..."); empty
	// means identity (writer == VM name).
	vmMap []string
}

func agentCmd() *cobra.Command {
	var cfg config
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Run the per-node HA agent (one per replicated-volume consumer)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := cfg.validate(); err != nil {
				return fmt.Errorf("invalid config: %w", err)
			}
			return runAgent(cmd.Context(), cfg)
		},
	}
	f := cmd.Flags()
	f.StringVar(&cfg.nodeName, "node-name", "", "unique node id within the cluster (also this node's fencing identity)")
	f.StringVar(&cfg.clusterName, "cluster-name", "", "logical volume/cluster name (namespaces etcd keys)")
	f.StringSliceVar(&cfg.etcd, "etcd", nil, "etcd endpoints (comma-separated)")
	f.IntVar(&cfg.etcdTTLSec, "etcd-session-ttl", 15, "etcd lease TTL in seconds (failover floor)")
	f.StringSliceVar(&cfg.replicas, "replica", nil, "replica NBD endpoint name=host:port[/export] (repeatable)")
	f.StringVar(&cfg.localName, "local-replica", "", "name of the replica reads prefer (same-host)")
	f.IntVar(&cfg.minInSync, "min-in-sync", 1, "minimum in-sync replicas a write must reach")
	f.DurationVar(&cfg.nbdTimeout, "nbd-timeout", 0, "per-op NBD deadline for replica clients (0 = none)")
	f.StringVar(&cfg.serveAddr, "serve", "", "local/WireGuard address to serve the gated ActiveDevice over NBD (empty = do not serve)")
	f.StringVar(&cfg.exportName, "export", "", "NBD export name for the served device (empty = default export)")
	f.DurationVar(&cfg.dialTimeout, "dial-timeout", 10*time.Second, "timeout for dialing replica NBD endpoints")
	f.StringVar(&cfg.weftEndpoint, "weft-agent", "", "weft-agent gRPC endpoint for fencing (host:port) — REQUIRED")
	f.StringVar(&cfg.weftProject, "weft-project", "", "weft project hosting the writer microVMs")
	f.StringVar(&cfg.weftTLSCA, "weft-tls-ca", "", "PEM CA bundle to verify the weft-agent server cert (REQUIRED unless --weft-insecure)")
	f.StringVar(&cfg.weftTLSCert, "weft-tls-cert", "", "client cert for mTLS to the weft-agent (optional; pair with --weft-tls-key)")
	f.StringVar(&cfg.weftTLSKey, "weft-tls-key", "", "client key for mTLS to the weft-agent (optional; pair with --weft-tls-cert)")
	f.StringVar(&cfg.weftTLSServer, "weft-tls-server-name", "", "override SNI/ServerName for cert verification (defaults to the endpoint host)")
	f.BoolVar(&cfg.weftInsecure, "weft-insecure", false, "dial the weft-agent without TLS (LOUD warning; only over WireGuard/SSH-tunnel; NEVER in production)")
	f.DurationVar(&cfg.fenceTimeout, "fence-timeout", 30*time.Second, "wait-for-stopped timeout during fencing")
	f.StringSliceVar(&cfg.vmMap, "vm-map", nil, "writer-id=vm-name mappings (comma-separated; default identity)")
	return cmd
}

func (c config) validate() error {
	if c.nodeName == "" {
		return errors.New("--node-name is required")
	}
	if c.clusterName == "" {
		return errors.New("--cluster-name is required")
	}
	if len(c.etcd) == 0 {
		return errors.New("--etcd is required")
	}
	if len(c.replicas) == 0 {
		return errors.New("at least one --replica is required")
	}
	if c.weftEndpoint == "" {
		return errors.New("--weft-agent is required (a replicated volume MUST be able to fence its prior writer)")
	}
	return nil
}

func runAgent(ctx context.Context, cfg config) error {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// 1. Dial the replica NBD endpoints into volume.Devices.
	reps, closeReps, err := dialReplicas(ctx, cfg, log)
	if err != nil {
		return err
	}
	defer closeReps()

	// 2. Build the replication engine (the data plane).
	eng, err := replica.New(reps, replica.Config{MinInSync: cfg.minInSync, Local: cfg.localName})
	if err != nil {
		return fmt.Errorf("replica engine: %w", err)
	}

	// 3. Coordinator: etcd lease election + lease-bound membership.
	coord, err := etcd.New(etcd.Config{
		Endpoints:         cfg.etcd,
		Cluster:           cfg.clusterName,
		NodeID:            cfg.nodeName,
		SessionTTLSeconds: cfg.etcdTTLSec,
	}, clientv3.Config{Endpoints: cfg.etcd, DialTimeout: 5 * time.Second}, log)
	if err != nil {
		return fmt.Errorf("coordinator: %w", err)
	}
	defer func() { _ = coord.Close() }()

	// 4. Fencer: weft STONITH.
	fencer, closeFencer, err := buildFencer(cfg, log)
	if err != nil {
		return err
	}
	defer closeFencer()

	// 5. Controller: fence-before-promote, gated ActiveDevice.
	ctrl, dev, err := replicaha.New(eng, coord, fencer, log)
	if err != nil {
		return fmt.Errorf("controller: %w", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = ctrl.Stop(sctx)
	}()

	// 6. Expose the gated device to the local consumer over NBD. WriteAt/Sync
	// return ErrNotLeader until this node is the confirmed leader; the consumer
	// just retries / fails over. If --serve is empty, the device is available
	// in-process only (document the sink for an embedded consumer).
	if cfg.serveAddr != "" {
		srvClose, err := serveDevice(cfg, dev, log)
		if err != nil {
			return err
		}
		defer srvClose()
	} else {
		log.Warn("no --serve address; the gated ActiveDevice is exposed in-process only")
	}

	log.Info("weft-ha-block agent started",
		"node", cfg.nodeName, "cluster", cfg.clusterName,
		"replicas", len(reps), "serve", cfg.serveAddr)

	if err := ctrl.Run(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, replicaha.ErrStopped) {
		return err
	}
	return nil
}

// dialReplicas connects each --replica "name=addr[/export]" into an NBD client
// and returns the replica set plus a closer.
func dialReplicas(ctx context.Context, cfg config, log *slog.Logger) ([]replica.Replica, func(), error) {
	var clients []*govnbd.Client
	closeAll := func() {
		for _, c := range clients {
			_ = c.Close()
		}
	}
	reps := make([]replica.Replica, 0, len(cfg.replicas))
	for _, spec := range cfg.replicas {
		name, addr, export, err := parseReplica(spec)
		if err != nil {
			closeAll()
			return nil, nil, err
		}
		dctx, cancel := context.WithTimeout(ctx, cfg.dialTimeout)
		var opts []govnbd.ClientOption
		if cfg.nbdTimeout > 0 {
			opts = append(opts, govnbd.WithTimeout(cfg.nbdTimeout))
		}
		cli, err := govnbd.DialExport(dctx, addr, export, opts...)
		cancel()
		if err != nil {
			closeAll()
			return nil, nil, fmt.Errorf("dial replica %q (%s): %w", name, addr, err)
		}
		clients = append(clients, cli)
		reps = append(reps, replica.Replica{Name: name, Dev: cli})
		log.Info("replica dialed", "name", name, "addr", addr, "export", export)
	}
	return reps, closeAll, nil
}

// parseReplica splits "name=host:port[/export]".
func parseReplica(spec string) (name, addr, export string, err error) {
	eq := indexByte(spec, '=')
	if eq <= 0 || eq == len(spec)-1 {
		return "", "", "", fmt.Errorf("bad --replica %q: want name=host:port[/export]", spec)
	}
	name = spec[:eq]
	rest := spec[eq+1:]
	if slash := indexByte(rest, '/'); slash >= 0 {
		addr, export = rest[:slash], rest[slash+1:]
	} else {
		addr = rest
	}
	if addr == "" {
		return "", "", "", fmt.Errorf("bad --replica %q: empty address", spec)
	}
	return name, addr, export, nil
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// buildFencer wires the weft STONITH fencer per the TLS gate.
func buildFencer(cfg config, log *slog.Logger) (replica.Fencer, func(), error) {
	nameFor := parseVMMap(cfg.vmMap)
	var stopper *fencing.GRPCStopper
	switch {
	case cfg.weftTLSCA != "":
		tlsCfg, err := fencing.LoadClientTLSConfig(cfg.weftTLSCA, cfg.weftTLSCert, cfg.weftTLSKey, cfg.weftTLSServer)
		if err != nil {
			return nil, nil, fmt.Errorf("weft-tls: %w", err)
		}
		stopper = fencing.NewGRPCStopperTLS(cfg.weftEndpoint, cfg.weftProject, tlsCfg, log)
		log.Info("fencer wired with TLS", "endpoint", cfg.weftEndpoint, "ca", cfg.weftTLSCA, "mtls", cfg.weftTLSCert != "")
	case cfg.weftInsecure:
		stopper = fencing.NewGRPCStopper(cfg.weftEndpoint, cfg.weftProject, log)
		log.Warn("fencer wired WITHOUT TLS (--weft-insecure); MITM can swallow StopVM and cause split-brain; only over WireGuard/SSH-tunnel; NEVER in production")
	default:
		return nil, nil, errors.New("fencer: --weft-agent requires either --weft-tls-ca <path> (recommended) or --weft-insecure (dev only); refusing to start with an unauthenticated fencer that could silently split-brain")
	}
	f := fencing.NewVMFencer(stopper, nameFor, cfg.fenceTimeout, log)
	return f, func() { _ = stopper.Close() }, nil
}

// parseVMMap turns "id=vm,id2=vm2" into a VMNameFunc; nil/empty => identity.
func parseVMMap(specs []string) fencing.VMNameFunc {
	if len(specs) == 0 {
		return nil
	}
	m := make(map[string]string, len(specs))
	for _, s := range specs {
		if eq := indexByte(s, '='); eq > 0 {
			m[s[:eq]] = s[eq+1:]
		}
	}
	return func(writer string) string {
		if vm, ok := m[writer]; ok {
			return vm
		}
		return writer
	}
}

// serveDevice exposes dev (the gated ActiveDevice) over NBD on cfg.serveAddr.
func serveDevice(cfg config, dev *replicaha.ActiveDevice, log *slog.Logger) (func(), error) {
	ln, err := net.Listen("tcp", cfg.serveAddr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", cfg.serveAddr, err)
	}
	srv := &govnbd.Server{
		Exports: []govnbd.Export{{Name: cfg.exportName, Device: dev}},
		Log:     nbdLogger{log},
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Error("nbd serve ended", "err", err)
		}
	}()
	log.Info("serving gated ActiveDevice over NBD", "addr", cfg.serveAddr, "export", cfg.exportName)
	return func() { _ = ln.Close() }, nil
}

// nbdLogger adapts slog to the nbd.Logger Printf contract.
type nbdLogger struct{ log *slog.Logger }

func (l nbdLogger) Printf(format string, v ...any) { l.log.Warn(fmt.Sprintf(format, v...)) }
