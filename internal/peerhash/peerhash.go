// Package peerhash derives the per-viewer peer access_hash a client carries
// back to name a peer it has been shown.
//
// The value is a keyed MAC over a versioned label and the (viewer, kind, peer)
// triple, truncated to 64 bits. It is stateless: nothing is stored, and any
// process holding the same key material reproduces it. Keying on the pair
// rather than the peer is what makes a leaked hash inert in a stranger's hands
// — the pair a viewer was shown does not verify for any other viewer. Including
// the kind separates the id namespaces, which overlap routinely.
//
// # Key rotation constraint
//
// The derivation deliberately carries no key epoch. It is a pure function of
// the subkey, so any change of master key material invalidates every hash every
// live client holds. That is free today and only today: rotating the master is
// already a total re-auth event, because the stored auth-key blob is versioned
// by format but not by key identity, so after a rotation no stored key opens,
// every client re-handshakes and re-fetches its peers, and the hash
// invalidation is fully subsumed.
//
// THE CONSTRAINT THIS PLACES ON WHOEVER IMPLEMENTS KEY ROTATION: the day
// keycrypt gains dual-key rotation — any scheme where a live session survives a
// master key change — the peer hash must gain an epoch or an accept-previous
// window in that same change. Otherwise the rotation leaves every session alive
// while silently invalidating every peer reference it has cached, and every
// subsequent request naming a peer fails validation with no signal that a
// rotation caused it. An epoch is also the only mechanism that would let a
// re-derivation be recognised as the same reference across a key change.
package peerhash

import (
	"crypto/hkdf"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const (
	// SubkeyLen is the derived subkey length, matching the HMAC-SHA256 block
	// input size the derivation uses.
	SubkeyLen = 32
	// masterLen is the master key length Subkey requires. It matches
	// keycrypt.KeyLen, but is stated here rather than imported: this package
	// must not pull the storage cipher into the RPC layer's dependency graph.
	masterLen = 32
	// subkeyInfo domain-separates the peer-hash subkey from every other use of
	// the master key material. Changing it invalidates every issued hash.
	subkeyInfo = "telegram-server/peer-access-hash/subkey/v1"
	// label prefixes the MAC input, versioning the construction itself. The
	// fields after it are all fixed-width, so the encoding is unambiguous
	// without a length prefix.
	label = "tg-peer-access-hash-v1"
)

// Kind discriminates the peer id namespace. The values are wire-frozen: they
// are MAC input, so changing one invalidates every hash issued for that kind.
// They mirror store.PeerType by value, without importing it.
type Kind uint32

const (
	KindUser    Kind = 1
	KindChat    Kind = 2
	KindChannel Kind = 3
)

// Subkey derives the peer-hash subkey from the master key material at process
// start. Only the result may travel to the RPC layer; the master key's reach
// stays what it is today, which is storage.
func Subkey(master []byte) ([]byte, error) {
	if len(master) != masterLen {
		return nil, fmt.Errorf("peerhash: master key must be %d bytes, got %d", masterLen, len(master))
	}
	// No salt: the master key is already full-entropy 32-byte key material, so
	// this is a domain-separating expansion, not a randomness extraction.
	sub, err := hkdf.Key(sha256.New, master, nil, subkeyInfo, SubkeyLen)
	if err != nil {
		return nil, fmt.Errorf("peerhash: derive subkey: %w", err)
	}
	return sub, nil
}

// Deriver issues peer access hashes under one subkey. It is immutable after
// construction and safe for concurrent use.
type Deriver struct {
	key []byte
}

// New builds a Deriver from a subkey produced by Subkey. It fails fast on a
// wrong-length key so a misconfigured server never starts issuing hashes under
// weak key material.
func New(subkey []byte) (*Deriver, error) {
	if len(subkey) != SubkeyLen {
		return nil, fmt.Errorf("peerhash: subkey must be %d bytes, got %d", SubkeyLen, len(subkey))
	}
	return &Deriver{key: append([]byte(nil), subkey...)}, nil
}

// Derive returns the access_hash the viewer carries for this peer. It is the
// single entry point: a hash issued through it verifies through it, and no
// other construction of a peer access_hash may exist in the server.
func (d *Deriver) Derive(viewerID int64, kind Kind, peerID int64) int64 {
	var msg [len(label) + 8 + 4 + 8]byte
	n := copy(msg[:], label)
	putInt64(msg[n:], viewerID)
	binary.BigEndian.PutUint32(msg[n+8:], uint32(kind))
	putInt64(msg[n+12:], peerID)

	mac := hmac.New(sha256.New, d.key)
	// hash.Hash never returns an error from Write.
	mac.Write(msg[:])
	sum := mac.Sum(nil)
	// Truncate to 64 bits by taking the leading eight bytes. access_hash is an
	// int64 on the wire and every bit pattern is a legal value, so the whole
	// range is used rather than masked into a positive half.
	return int64(binary.BigEndian.Uint64(sum[:8])) //nolint:gosec // reinterpreting all 64 bits, not narrowing
}

// putInt64 writes v big-endian into b. The unsigned conversion reinterprets the
// two's-complement bits rather than narrowing them, so no value is lost and no
// two ids share an encoding — which is what the derivation needs.
func putInt64(b []byte, v int64) {
	binary.BigEndian.PutUint64(b, uint64(v)) //nolint:gosec // reinterpreting all 64 bits, not narrowing
}
