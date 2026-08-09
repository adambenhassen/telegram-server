package store

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErrNoClientAddr is returned when a per-IP check is handed a request whose
// connection carried no usable address. Every TCP peer has one, and the one a
// handler sees is read off the socket before any client byte is interpreted, so
// this is a transport-layer failure and never something a client can provoke.
// It is a distinct error because the caller — not the store — decides what to
// do with a request it cannot attribute to a network.
var ErrNoClientAddr = errors.New("no client address")

const (
	// ipv4BucketBits buckets an IPv4 client at its own address.
	ipv4BucketBits = 32
	// ipv6BucketBits buckets an IPv6 client at its /64. A /128 key is not a
	// limit: a host on a normal v6 allocation mints fresh addresses inside its
	// own /64 for free, so the /64 is the smallest unit it cannot get more of.
	ipv6BucketBits = 64
	// sendCodeIPLockClass namespaces this limiter's advisory locks. Two-argument
	// advisory locks occupy a space of their own, disjoint from the
	// single-argument ones CheckAndChargeLookup takes.
	sendCodeIPLockClass = 0x74674950 // "tgIP"
)

// SendCodeIPLimits holds the two counters that bound auth.sendCode per client
// network. Both are charged against the same bucket key, and either may be
// disabled on its own by a zero limit or window.
type SendCodeIPLimits struct {
	// Calls caps how many sendCode requests one key may make per window.
	Calls RateLimitConfig
	// Phones caps how many distinct phone numbers one key may request codes for
	// per window. It is what bounds a spray across many numbers, which the call
	// counter alone would only slow down.
	Phones RateLimitConfig
}

// Enabled reports whether either counter enforces anything.
func (l SendCodeIPLimits) Enabled() bool {
	return l.Calls.enabled() || l.Phones.enabled()
}

// IPBucketKey reduces a client address to the network the per-IP limits are
// keyed on.
//
// It reports false for an address the transport could not parse. No caller may
// turn that into a key: every unattributable request would land in one shared
// bucket, and a single such peer would then spend the limit for all of them.
func IPBucketKey(addr netip.Addr) (netip.Prefix, bool) {
	if !addr.IsValid() {
		return netip.Prefix{}, false
	}
	// A 4-in-6 address is the same host as its IPv4 form and must not get a
	// second bucket; a zone names a local interface, not a different network.
	addr = addr.Unmap().WithZone("")
	bits := ipv4BucketBits
	if addr.Is6() {
		bits = ipv6BucketBits
	}
	return netip.PrefixFrom(addr, bits).Masked(), true
}

// CheckAndChargeSendCodeIP checks and charges both per-IP counters for one
// auth.sendCode call arriving from addr for phone.
//
// It returns nil when the call is allowed, and a RateLimitResult carrying the
// remaining wait when it is denied. A denied call has written nothing: both
// counters are charged inside one transaction, so a call the second counter
// refuses does not leave the first one's token spent. The wait is derived only
// from rows keyed on the network, never from anything about the phone number,
// which is what keeps the denial from becoming an account-existence oracle.
//
// Locking: the transaction takes one advisory lock on the bucket key before it
// touches either table, so concurrent calls from one network are serialised and
// neither counter can be overshot by a burst. It is the first lock the
// transaction takes and the only one held across both tables; nothing else in
// the package touches either table, so it cannot participate in a cycle.
func (s *Store) CheckAndChargeSendCodeIP(ctx context.Context, addr netip.Addr, phone string, cfg SendCodeIPLimits) (*RateLimitResult, error) {
	if !cfg.Enabled() {
		return nil, nil //nolint:nilnil // disabled config is not an error
	}
	key, ok := IPBucketKey(addr)
	if !ok {
		return nil, ErrNoClientAddr
	}
	phone = NormalizePhone(phone)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock($1, hashtext($2))",
		sendCodeIPLockClass, key.String(),
	); err != nil {
		return nil, fmt.Errorf("advisory lock: %w", err)
	}
	qtx := s.q.WithTx(tx)

	// Call counter first, then distinct numbers: the order decides which wait a
	// call over both limits is told, and the call window is the shorter one.
	denied, err := chargeSendCodeIPCall(ctx, qtx, key, cfg.Calls)
	if err != nil {
		return nil, err
	}
	if denied == nil {
		denied, err = chargeSendCodeIPPhone(ctx, qtx, key, phone, cfg.Phones)
		if err != nil {
			return nil, err
		}
	}
	if denied != nil {
		// Roll back by returning: the deferred Rollback undoes the token the
		// call counter may already have consumed above.
		return denied, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return nil, nil //nolint:nilnil // allowed is not an error
}

// chargeSendCodeIPCall consumes one token from the key's fixed-window call
// counter, returning the remaining wait when the window is already spent.
func chargeSendCodeIPCall(ctx context.Context, q *db.Queries, key netip.Prefix, cfg RateLimitConfig) (*RateLimitResult, error) {
	if !cfg.enabled() {
		return nil, nil //nolint:nilnil // disabled counter is not an error
	}
	_, err := q.TryConsumeSendCodeIPCall(ctx, db.TryConsumeSendCodeIPCallParams{
		IpKey:      key,
		Column2:    pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
		TokenCount: int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
	})
	if err == nil {
		return nil, nil //nolint:nilnil // allowed is not an error
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("consume send code ip call: %w", err)
	}
	// The upsert above locked the row that refused it and holds that lock to
	// the end of this transaction, so the deadline read here is the one that
	// denied the call and cannot be swept out from under it.
	expiresAt, err := q.GetSendCodeIPCallExpiry(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("send code ip call expiry: %w", err)
	}
	return &RateLimitResult{Wait: waitUntil(expiresAt.Time)}, nil
}

// chargeSendCodeIPPhone charges one distinct phone number to the key, returning
// the wait until the key's oldest number frees a slot when the quota is spent.
func chargeSendCodeIPPhone(ctx context.Context, q *db.Queries, key netip.Prefix, phone string, cfg RateLimitConfig) (*RateLimitResult, error) {
	if !cfg.enabled() {
		return nil, nil //nolint:nilnil // disabled counter is not an error
	}
	// Prune before counting, so retention is the window itself rather than
	// whatever the periodic sweep last reached.
	if err := q.DeleteExpiredSendCodeIPPhones(ctx, key); err != nil {
		return nil, fmt.Errorf("prune send code ip phones: %w", err)
	}
	usage, err := q.GetSendCodeIPPhoneUsage(ctx, db.GetSendCodeIPPhoneUsageParams{IpKey: key, Phone: phone})
	if err != nil {
		return nil, fmt.Errorf("send code ip phone usage: %w", err)
	}
	// Already charged inside the window: repeating a number the key has spent a
	// slot on costs nothing. Its deadline is deliberately left alone — moving it
	// would keep the row, and the network-to-number pair it records, alive past
	// the window it was recorded in.
	if usage.Counted {
		return nil, nil //nolint:nilnil // allowed is not an error
	}
	if int(usage.Used) >= cfg.Limit {
		next, err := q.GetSendCodeIPPhoneNextExpiry(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("send code ip phone expiry: %w", err)
		}
		return &RateLimitResult{Wait: waitUntil(next.Time)}, nil
	}
	if err := q.InsertSendCodeIPPhone(ctx, db.InsertSendCodeIPPhoneParams{
		IpKey:   key,
		Phone:   phone,
		Column3: pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
	}); err != nil {
		return nil, fmt.Errorf("insert send code ip phone: %w", err)
	}
	return nil, nil //nolint:nilnil // allowed is not an error
}

// SweepExpiredSendCodeIPLimits deletes per-IP sendCode rows past their
// deadline: counter rows whose window has closed, and phone rows past their
// retention window. A key that stays active prunes its own phone rows on write;
// this is what clears the ones that go quiet. Returns the total rows deleted.
func (s *Store) SweepExpiredSendCodeIPLimits(ctx context.Context) (int64, error) {
	calls, err := s.q.SweepExpiredSendCodeIPCalls(ctx)
	if err != nil {
		return 0, fmt.Errorf("sweep send code ip calls: %w", err)
	}
	phones, err := s.q.SweepExpiredSendCodeIPPhones(ctx)
	if err != nil {
		return calls, fmt.Errorf("sweep send code ip phones: %w", err)
	}
	return calls + phones, nil
}

// waitUntil is the client-visible wait for a deadline: whole seconds, rounded
// up, never below one. time.Until reads the Go clock against a Postgres
// timestamp; the error is bounded by the app/DB clock offset, negligible on a
// single host.
func waitUntil(deadline time.Time) time.Duration {
	wait := time.Until(deadline)
	secs := int(wait / time.Second)
	if wait%time.Second > 0 {
		secs++
	}
	return time.Duration(max(secs, 1)) * time.Second
}
