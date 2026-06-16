// Package etcd implements the replica-ha Coordinator seam over etcd's
// concurrency.Election. It is the coordination plane for go-volumes
// replicated-volume HA: lease-based leader election plus lease-bound
// membership, so that the safety-critical replicaha.Controller can keep
// exactly one node the active writer for a volume.
//
// This package is a weft-side INTEGRATOR of go-volumes: it imports
// github.com/go-volumes/replica-ha and satisfies its Coordinator interface; it
// does not modify go-volumes. It mirrors the etcd handling of
// weft-ha-postgresql's internal/dcs (NewSession/NewElection/Campaign/Resign/
// Observe/Leader/MemberList) but adapts the stream shape to replicaha.Leadership.
package etcd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"sync"
	"time"

	replicaha "github.com/go-volumes/replica-ha"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// Compile-time assertion that *Coordinator satisfies the seam.
var _ replicaha.Coordinator = (*Coordinator)(nil)

// Coordinator is the etcd-backed replicaha.Coordinator for one volume.
//
// Leadership is a concurrency.Election keyed under
// /weft-ha-block/<cluster>/leader; the election value is the campaigning node's
// ID. Membership is a lease-bound key under /weft-ha-block/<cluster>/members/
// written with the SAME session lease as the election, so a fenced or
// partitioned node drops out of Members automatically when its lease expires —
// exactly the live-peers semantics the seam requires.
//
// The session TTL is the failover floor: a node that stops renewing its lease
// (process stall, partition, being STONITH'd by a peer) loses leadership when
// the lease expires at the server even if it never resigns cleanly. That loss
// surfaces on Observe as IsSelf==false followed by channel close, which is what
// the Controller relies on to stop writing.
type Coordinator struct {
	endpoints  []string
	cluster    string
	nodeID     string
	sessionTTL int
	log        *slog.Logger

	dialCfg clientv3.Config // prepared dial config (endpoints, TLS, timeouts)

	mu       sync.Mutex
	client   *clientv3.Client
	session  *concurrency.Session
	election *concurrency.Election
}

// Config configures a Coordinator.
type Config struct {
	// Endpoints are the etcd client endpoints (e.g. ["https://etcd-1:2379"]).
	Endpoints []string
	// Cluster is the logical volume/cluster name; it namespaces the etcd keys
	// so several HA volumes can share one etcd cluster.
	Cluster string
	// NodeID is this node's stable identity. It is the value written to the
	// election key when this node is leader, and the identity a peer passes to
	// the Fencer to STONITH this node. Must be unique and non-empty.
	NodeID string
	// SessionTTLSeconds is the lease TTL. It bounds the failover window: a
	// fenced/partitioned leader's lease expires after at most this many seconds.
	// Defaults to 15 when <= 0. Pick 10 (snappy, tight on jitter) .. 30 (safe
	// under WAN flapping).
	SessionTTLSeconds int
}

// New builds a Coordinator. It does not dial etcd eagerly; the first Campaign/
// Observe/Members call opens the client and session so the agent can start even
// while the store is briefly unreachable. If log is nil a discarding logger is
// used. NodeID is required.
func New(cfg Config, dialCfg clientv3.Config, log *slog.Logger) (*Coordinator, error) {
	if cfg.NodeID == "" {
		return nil, errors.New("etcd coordinator: empty NodeID")
	}
	if len(cfg.Endpoints) == 0 && len(dialCfg.Endpoints) == 0 {
		return nil, errors.New("etcd coordinator: no endpoints")
	}
	ttl := cfg.SessionTTLSeconds
	if ttl <= 0 {
		ttl = 15
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	eps := cfg.Endpoints
	if len(eps) == 0 {
		eps = dialCfg.Endpoints
	}
	c := &Coordinator{
		endpoints:  eps,
		cluster:    cfg.Cluster,
		nodeID:     cfg.NodeID,
		sessionTTL: ttl,
		log:        log,
	}
	// Stash the prepared dial config (TLS, dial timeout) for lazy connect.
	c.dialCfg = dialCfg
	if len(c.dialCfg.Endpoints) == 0 {
		c.dialCfg.Endpoints = eps
	}
	if c.dialCfg.DialTimeout == 0 {
		c.dialCfg.DialTimeout = 5 * time.Second
	}
	return c, nil
}

// connect lazily opens the etcd client + session and rebuilds a dead session
// (lease expiry / partition) transparently. The session owns the lease that the
// election and membership keys hang off, so losing the session means losing
// leadership automatically — which is exactly what we want when a node is fenced.
func (c *Coordinator) connect(ctx context.Context) (*concurrency.Session, *concurrency.Election, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil && c.election != nil {
		// Liveness check: the session's Done() channel closes when the lease
		// expires or KeepAlive can no longer reach etcd. A dead session must be
		// rebuilt or a recovered node would keep talking to a corpse and never
		// re-campaign.
		select {
		case <-c.session.Done():
			c.log.Warn("etcd session lost (lease expired or partitioned) — rebuilding")
			c.session = nil
			c.election = nil
		default:
			return c.session, c.election, nil
		}
	}
	if c.client == nil {
		cli, err := clientv3.New(c.dialCfg)
		if err != nil {
			return nil, nil, fmt.Errorf("etcd client: %w", err)
		}
		c.client = cli
	}
	sess, err := concurrency.NewSession(c.client,
		concurrency.WithTTL(c.sessionTTL),
		concurrency.WithContext(ctx),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("etcd session: %w", err)
	}
	c.session = sess
	c.election = concurrency.NewElection(sess, c.electionPrefix())
	return sess, c.election, nil
}

func (c *Coordinator) electionPrefix() string {
	return path.Join("/weft-ha-block", c.cluster, "leader")
}

func (c *Coordinator) membersPrefix() string {
	return path.Join("/weft-ha-block", c.cluster, "members") + "/"
}

// NodeID returns this node's configured identity.
func (c *Coordinator) NodeID() string { return c.nodeID }

// Campaign blocks until this node acquires and holds the lease, or ctx is done.
// It also (re-)announces this node's membership key under the session lease so
// peers can discover it via Members. Re-campaigning while leader returns once
// the election value is already this node.
func (c *Coordinator) Campaign(ctx context.Context) error {
	sess, elec, err := c.connect(ctx)
	if err != nil {
		return err
	}
	// Announce membership eagerly so a node that is campaigning (not yet leader)
	// is already a visible live peer. The key carries the session lease, so it
	// vanishes when the node is fenced.
	if err := c.announce(ctx, sess); err != nil {
		return err
	}
	if err := elec.Campaign(ctx, c.nodeID); err != nil {
		return fmt.Errorf("campaign: %w", err)
	}
	c.log.Info("won election", "node", c.nodeID, "cluster", c.cluster)
	return nil
}

// announce writes this node's membership key with the session lease. Idempotent.
func (c *Coordinator) announce(ctx context.Context, sess *concurrency.Session) error {
	key := c.membersPrefix() + c.nodeID
	if _, err := c.client.Put(ctx, key, c.nodeID, clientv3.WithLease(sess.Lease())); err != nil {
		return fmt.Errorf("announce member: %w", err)
	}
	return nil
}

// Resign voluntarily releases the lease so a peer can win. A no-op when this
// node is not the current election leader.
func (c *Coordinator) Resign(ctx context.Context) error {
	c.mu.Lock()
	elec := c.election
	c.mu.Unlock()
	if elec == nil {
		return nil
	}
	if err := elec.Resign(ctx); err != nil {
		return fmt.Errorf("resign: %w", err)
	}
	c.log.Info("resigned leadership", "node", c.nodeID)
	return nil
}

// Observe streams a replicaha.Leadership on every leadership change for as long
// as ctx is live.
//
// Mapping etcd's election.Observe -> Leadership:
//
//   - Each observed leader value val yields
//     Leadership{Leader: val, IsSelf: val == NodeID, Term: <leader key
//     CreateRevision>}. CreateRevision is strictly greater on every new leader
//     (a fresh election key is created), so it is a monotonically non-decreasing
//     fencing token / epoch — exactly the Term contract.
//   - SAFETY HINGE: when the session's lease is lost (session.Done() fires) or
//     the underlying observe stream closes (store unreachable), we emit a final
//     Leadership{IsSelf:false} and CLOSE the channel. The Controller treats both
//     the IsSelf:false observation and the channel close as "stop writing", so a
//     partitioned-away old leader demotes before its lease grace window elapses.
//   - ctx cancellation closes the channel cleanly.
func (c *Coordinator) Observe(ctx context.Context) (<-chan replicaha.Leadership, error) {
	sess, elec, err := c.connect(ctx)
	if err != nil {
		return nil, err
	}
	raw := elec.Observe(ctx)
	out := make(chan replicaha.Leadership, 4)
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case <-sess.Done():
				// Lease lost / session dead: we can no longer trust our
				// leadership. Surface a demotion and stop. This is what stops
				// the writer before the grace window elapses.
				c.emit(ctx, out, replicaha.Leadership{IsSelf: false})
				return
			case resp, ok := <-raw:
				if !ok {
					// Observe stream ended (session ended / store unreachable):
					// same safety handling as a lost lease.
					c.emit(ctx, out, replicaha.Leadership{IsSelf: false})
					return
				}
				if len(resp.Kvs) == 0 {
					continue
				}
				kv := resp.Kvs[0]
				leader := string(kv.Value)
				c.emit(ctx, out, replicaha.Leadership{
					Leader: leader,
					IsSelf: leader == c.nodeID,
					Term:   kv.CreateRevision,
				})
			}
		}
	}()
	return out, nil
}

// emit delivers one Leadership unless ctx is done; it never blocks past ctx.
func (c *Coordinator) emit(ctx context.Context, out chan<- replicaha.Leadership, l replicaha.Leadership) {
	select {
	case out <- l:
	case <-ctx.Done():
	}
}

// Members lists the node IDs holding a live membership lease. Fenced or
// partitioned nodes drop out as their lease expires, so the returned set is the
// live, reachable peers.
func (c *Coordinator) Members(ctx context.Context) ([]string, error) {
	if _, _, err := c.connect(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	cli := c.client
	c.mu.Unlock()
	resp, err := cli.Get(ctx, c.membersPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("list members: %w", err)
	}
	out := make([]string, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		out = append(out, string(kv.Value))
	}
	return out, nil
}

// Close releases the session (dropping the lease) and the etcd client. Safe to
// call more than once.
func (c *Coordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.session != nil {
		_ = c.session.Close()
		c.session = nil
		c.election = nil
	}
	if c.client != nil {
		err := c.client.Close()
		c.client = nil
		return err
	}
	return nil
}
