// Package srp implements the server side of Telegram's SRP-6a cloud-password
// protocol (two-factor auth). The client half lives in gotd's crypto/srp; this
// package performs the complementary group math so a real gotd client can be
// challenged and verified byte-for-byte.
//
// See https://core.telegram.org/api/srp. All wire integers are big-endian,
// left-zero-padded to 256 bytes. The password itself is never seen server-side:
// the client computes the verifier v = g^x mod p and the server stores only v.
package srp

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"math/big"

	"github.com/gotd/td/crypto"
)

// G is the Telegram SRP generator. It is a fixed protocol constant, not a
// choice: gotd's client validates it via crypto.CheckDH.
const G = 3

// primeHex is Telegram's canonical 2048-bit safe prime p, shared with the DH
// handshake. It is a fixed protocol constant; gotd validates it client-side.
const primeHex = "" +
	"C71CAEB9C6B1C9048E6C522F70F13F73980D40238E3E21C14934D037563D930F" +
	"48198A0AA7C14058229493D22530F4DBFA336F6E0AC925139543AED44CCE7C37" +
	"20FD51F69458705AC68CD4FE6B6B13ABDC9746512969328454F18FAF8C595F64" +
	"2477FE96BB2A941D5BCD1D4AC8CC49880708FA9B378E3C4F3A9060BEE67CF9A4" +
	"A4A695811051907E162753B56B0F6B410DBA74D8A84B2A14B3144E0EF1284754" +
	"FD17ED950D5965B4B9DD46582DB1178D169C6BC465B0D6FF9CA3928FEF5B9AE4" +
	"E418FC15E83EBEA0F87FA9FF5EED70050DED2849F47BF959D956850CE929851F" +
	"0D8115F635B105EE2E4E15D04B2454BF6F4FADF034B10403119CD8E3B92FCC5B"

// PadLen is the fixed byte length of every SRP wire integer.
const PadLen = 256

var (
	// p is the modulus; gBytes and pBytes are its padded byte forms.
	p      *big.Int
	pBytes []byte
	gBytes []byte
	// k = H(p ‖ pad256(g)) is the SRP-6a multiplier, precomputed once.
	k *big.Int
	// hpXorHg = H(p) ⊕ H(pad256(g)), the first block of M1, precomputed once.
	hpXorHg []byte
)

func init() {
	var ok bool
	p, ok = new(big.Int).SetString(primeHex, 16)
	if !ok {
		panic("srp: invalid canonical prime")
	}
	// Fail fast if the embedded parameters are not the Telegram-valid group the
	// client will demand: a wrong p/g would make every login silently unverifiable.
	if err := crypto.CheckDH(G, p); err != nil {
		panic(fmt.Sprintf("srp: CheckDH failed for embedded params: %v", err))
	}
	pBytes = pad(p.Bytes())
	gBytes = pad(big.NewInt(G).Bytes())
	k = new(big.Int).SetBytes(hash(pBytes, gBytes))
	hp := sha256.Sum256(pBytes)
	hg := sha256.Sum256(gBytes)
	hpXorHg = xor(hp[:], hg[:])
}

// PBytes returns the padded 256-byte modulus for advertising in the KDF algo.
func PBytes() []byte { return append([]byte(nil), pBytes...) }

// ValidVerifier reports whether a client-supplied verifier v is a valid group
// element (0 < v < p). A degenerate v (e.g. 0) would make an SRP proof
// forgeable without the password, so it must be rejected before storage.
func ValidVerifier(verifier []byte) bool {
	v := new(big.Int).SetBytes(verifier)
	return v.Sign() > 0 && v.Cmp(p) < 0
}

// Challenge draws a fresh server secret b and returns B = (k·v + g^b) mod p,
// padded to 256 bytes, together with b (256 bytes) for the matching Verify. It
// rejects a degenerate B ≡ 0 (mod p). verifier is the stored SRP v.
func Challenge(verifier []byte) (bPub, bSecret []byte, err error) {
	b, err := randScalar()
	if err != nil {
		return nil, nil, err
	}
	v := new(big.Int).SetBytes(verifier)
	// B = (k*v + g^b) mod p
	gb := new(big.Int).Exp(big.NewInt(G), b, p)
	kv := new(big.Int).Mul(k, v)
	B := kv.Add(kv, gb)
	B.Mod(B, p)
	if B.Sign() == 0 {
		return nil, nil, errors.New("srp: degenerate B")
	}
	return pad(B.Bytes()), pad(b.Bytes()), nil
}

// Verify checks a client's SRP proof (A, M1) against the stored verifier and the
// challenge (bPub = B, bSecret = b) that produced it. It returns true only when
// the client proved knowledge of the password. Degenerate inputs (A ≡ 0, u == 0)
// fail closed as an invalid proof. The final compare is constant-time.
func Verify(verifier, salt1, salt2, aPub, m1, bPub, bSecret []byte) bool {
	// Reject noncanonical wire sizes up front: a real client always sends a
	// 256-byte A and a 32-byte M1. An oversized A would be truncated by pad for u
	// and M1 while its full value fed S, so refuse it rather than accept a
	// mismatched-length proof.
	if len(aPub) != PadLen || len(m1) != sha256.Size {
		return false
	}
	A := new(big.Int).SetBytes(aPub)
	// Reject A ≡ 0 (mod p): a zero A collapses S to 0 and forges any password.
	if new(big.Int).Mod(A, p).Sign() == 0 {
		return false
	}
	padA := pad(aPub)
	padB := pad(bPub)
	u := new(big.Int).SetBytes(hash(padA, padB))
	if u.Sign() == 0 {
		return false
	}
	v := new(big.Int).SetBytes(verifier)
	b := new(big.Int).SetBytes(bSecret)
	// S = (A · v^u)^b mod p
	vu := new(big.Int).Exp(v, u, p)
	base := new(big.Int).Mul(A, vu)
	base.Mod(base, p)
	S := base.Exp(base, b, p)
	kSess := hash(pad(S.Bytes()))
	// M1 = H( (H(p) ⊕ H(g)) ‖ H(salt1) ‖ H(salt2) ‖ pad256(A) ‖ pad256(B) ‖ H(S) )
	expected := hash(hpXorHg, hash(salt1), hash(salt2), padA, padB, kSess)
	return subtle.ConstantTimeCompare(expected, m1) == 1
}

// hash is the protocol hash H = SHA-256 over the concatenation of parts.
func hash(parts ...[]byte) []byte {
	h := sha256.New()
	for _, part := range parts {
		h.Write(part)
	}
	return h.Sum(nil)
}

// pad left-zero-pads b to 256 bytes (or returns the low 256 bytes if longer).
func pad(b []byte) []byte {
	out := make([]byte, PadLen)
	if len(b) >= PadLen {
		copy(out, b[len(b)-PadLen:])
		return out
	}
	copy(out[PadLen-len(b):], b)
	return out
}

// randScalar draws a fresh 2048-bit secret exponent from crypto/rand.
func randScalar() (*big.Int, error) {
	buf := make([]byte, PadLen)
	if _, err := rand.Read(buf); err != nil {
		return nil, fmt.Errorf("srp: rand: %w", err)
	}
	return new(big.Int).SetBytes(buf), nil
}

func xor(a, b []byte) []byte {
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out
}
