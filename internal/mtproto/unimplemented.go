package mtproto

import "time"

const (
	// unimplementedWindow is the span the per-connection budget on calls to
	// methods this server does not implement is counted over. It is a fixed
	// window, the same shape the store-backed rate limits use: a connection
	// that spent its budget a window ago has spent nothing, so the bound is a
	// rate rather than a quota on the life of the connection.
	//
	// Zero disables the budget entirely, matching the pre-auth bounds and the
	// rate-limit surfaces, and restores the unbounded call rate this replaced.
	unimplementedWindow = 60 * time.Second
	// unimplementedAnswerBudget is how many such calls a connection may make in
	// one window before it is told to back off. It is deliberately far above
	// what a real client asks for: a full client's cold start walks through the
	// methods it expects a server to have and never comes near it, so the first
	// bound a legitimate client meets is no bound at all.
	//
	// Zero disables the back-off step: every call inside the ceiling is then
	// answered as it would be with no bound.
	unimplementedAnswerBudget = 64
	// unimplementedCloseAt is where the connection ends instead. Four times the
	// budget in one window is not a client walking a method list, it is a loop,
	// and answering a loop is what makes it free to run.
	//
	// Closing is per connection on purpose: the peer may have another one, but
	// it pays a TCP connect, a transport negotiation and a key exchange for it,
	// which the pre-auth and unbound-key bounds already price. Zero disables the
	// close.
	unimplementedCloseAt = 256
	// unimplementedLogInterval bounds how often one connection's
	// not-implemented line is emitted. Without it the log is the second thing
	// the loop above costs, at a line and its attributes per call.
	unimplementedLogInterval = 10 * time.Second
)

// UnimplementedVerdict is what one call to a method this server does not
// implement is owed on the connection that made it.
type UnimplementedVerdict int

const (
	// UnimplementedAnswer is a call inside the budget: it is answered exactly as
	// it would be with no bound at all.
	UnimplementedAnswer UnimplementedVerdict = iota
	// UnimplementedFloodWait is a call past the budget: the caller is told to
	// wait, in the one way a client already understands. The answer to a call
	// inside the budget must not change to reach it — an error a client reads as
	// terminal turns a cheap loop into an expensive one.
	UnimplementedFloodWait
	// UnimplementedClose is a call past the ceiling: the connection ends, and
	// the caller is owed nothing back.
	UnimplementedClose
)

// unimplementedBudget counts the calls to methods this server does not
// implement made on one connection, and thins the line they produce.
//
// It holds no lock and needs none: every frame is decrypted and dispatched by
// its connection's single serve goroutine, the same one that owns Conn.created,
// so nothing else touches these counters. That is also how it keeps the
// ordering rule this package documents — it has nothing to order. The verdict
// is computed and returned, and the caller closes the socket after it, never
// under a lock this type holds.
type unimplementedBudget struct {
	// started is when the current window opened, zero before the first call.
	started time.Time
	// calls counts what has been charged inside that window.
	calls int
	// log thins the line those calls produce. Per connection, like the counter
	// above and for the same reason the pre-auth samplers are one per event: the
	// operator needs the line that says this is happening, and a window shared
	// across connections is a window one peer can spend on its own.
	log logSampler
	// closeLineWritten is set once the connection-ending verdict has written
	// its line. The close verdict is exempt from the interval gate, but a
	// connection ends exactly once, so the line is owed once: the first close
	// verdict takes it, and any later close verdict on the same conn — which a
	// test harness can drive past the ceiling — finds nothing left to write.
	closeLineWritten bool
}

// charge records one such call at now and reports what it is owed.
func (b *unimplementedBudget) charge(now time.Time) UnimplementedVerdict {
	if unimplementedWindow <= 0 {
		return UnimplementedAnswer
	}
	if b.started.IsZero() || now.Sub(b.started) >= unimplementedWindow {
		b.started, b.calls = now, 0
	}
	b.calls++
	switch {
	case unimplementedCloseAt > 0 && b.calls >= unimplementedCloseAt:
		return UnimplementedClose
	case unimplementedAnswerBudget > 0 && b.calls > unimplementedAnswerBudget:
		return UnimplementedFloodWait
	default:
		return UnimplementedAnswer
	}
}

// logAllow reports whether the line describing one such call may be emitted at
// now and, when it may, how many it stands for.
func (b *unimplementedBudget) logAllow(now time.Time) (int64, bool) {
	return b.log.allow(now, unimplementedLogInterval)
}

// logFlush reports the lines suppressed since the last one emitted, and owes a
// line only when there is a count to say. A burst ends the connection or stops,
// and a connection that ends or goes quiet owes no further line to flush the
// count — the count is lost unless the drop itself writes one. A drop with
// nothing suppressed writes nothing: the last line already stood for every
// call, and a zero-count line would be one the conn never owed.
func (b *unimplementedBudget) logFlush(now time.Time) (int64, bool) {
	return b.log.flush(now)
}

// logClose reports the suppressed count the connection-ending verdict's line
// stands for, and whether the line may be emitted. The close verdict is exempt
// from the interval gate, but the line is owed once per connection: the first
// close verdict consumes the pending count and takes the line, and any later
// close verdict on the same conn finds nothing left to write.
func (b *unimplementedBudget) logClose(now time.Time) (int64, bool) {
	if b.closeLineWritten {
		return 0, false
	}
	suppressed, _ := b.log.flush(now)
	b.closeLineWritten = true
	return suppressed, true
}

// ChargeUnimplemented records one call to a method this server does not
// implement on this connection and reports what it is owed. The caller decides
// what an answer looks like — the error a client sees is the RPC layer's, not
// the transport's — and ends the connection itself on UnimplementedClose.
//
// It must be called only for a frame that has already decrypted, like every
// other per-connection charge in this package: the budget is per connection and
// nothing that has not proved the key gets to spend one.
func (c *Conn) ChargeUnimplemented() UnimplementedVerdict {
	return c.unimplemented.charge(c.clock.Now())
}

// LogUnimplemented reports whether this connection's not-implemented line may
// be emitted now and, when it may, how many lines it stands for.
func (c *Conn) LogUnimplemented() (int64, bool) {
	return c.unimplemented.logAllow(c.clock.Now())
}

// LogUnimplementedClose reports the suppressed count the connection-ending
// verdict's line stands for, and whether the line may be emitted. The close
// verdict is exempt from the interval gate: a connection ends exactly once, so
// its line costs at most one line per connection, never one per call, and it is
// the one line that must say what was called rather than being lost behind the
// sampler. It emits at most once per connection — the first close verdict on
// the conn takes the line, and any later close verdict (the harness can drive
// past the ceiling) finds nothing left to write. It consumes the pending count
// the same way the drop's flush does, so a later FlushUnimplementedLog finds
// nothing left to write.
func (c *Conn) LogUnimplementedClose() (int64, bool) {
	return c.unimplemented.logClose(c.clock.Now())
}

// FlushUnimplementedLog writes the line this connection's not-implemented
// calls have been suppressed behind, when it owes one. It is owed by a
// connection that ends having spent its budget: the burst that fills the
// sampler ends the socket, and no later call on this conn comes to flush the
// count, so the count is lost unless the drop writes it. It is owed by a
// connection that goes quiet the same way: the sampler holds the count open
// until a later line, and a quiet conn has no later line.
//
// It writes at most one line, the same line the sampled path writes, and it
// never runs while a counter lock is held — there is none, the budget is
// touched only by the serve goroutine, and the caller writes it before the
// socket closes, not while closing it.
func (c *Conn) FlushUnimplementedLog() {
	suppressed, ok := c.unimplemented.logFlush(c.clock.Now())
	if !ok {
		return
	}
	c.log.Warn("method not implemented",
		"suppressed", suppressed,
	)
}
