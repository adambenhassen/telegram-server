package srp_test

import (
	"crypto/rand"
	"math/big"
	"testing"

	gotdsrp "github.com/gotd/td/crypto/srp"

	"github.com/adambenhassen/telegram-server/internal/srp"
)

// newInput mirrors the KDF algo the server advertises: server-chosen salts and
// the canonical group.
func newInput(t *testing.T) (salt1Base, salt2 []byte) {
	t.Helper()
	salt1Base = make([]byte, 32)
	salt2 = make([]byte, 32)
	if _, err := rand.Read(salt1Base); err != nil {
		t.Fatal(err)
	}
	if _, err := rand.Read(salt2); err != nil {
		t.Fatal(err)
	}
	return salt1Base, salt2
}

// newVerifier runs gotd's client set-password path to produce the stored
// verifier and the augmented salt1, exactly as a real client does on
// updatePasswordSettings.
func newVerifier(t *testing.T, password string, salt1Base, salt2 []byte) (verifier, salt1 []byte) {
	t.Helper()
	c := gotdsrp.NewSRP(rand.Reader)
	hash, augSalt1, err := c.NewHash([]byte(password), gotdsrp.Input{
		Salt1: salt1Base, Salt2: salt2, G: srp.G, P: srp.PBytes(),
	})
	if err != nil {
		t.Fatalf("client NewHash: %v", err)
	}
	return hash, augSalt1
}

// clientProof runs gotd's client login path against a server challenge b.
func clientProof(t *testing.T, password string, salt1, salt2, bPub []byte) (a, m1 []byte) {
	t.Helper()
	c := gotdsrp.NewSRP(rand.Reader)
	random := make([]byte, srp.PadLen)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	ans, err := c.Hash([]byte(password), bPub, random, gotdsrp.Input{
		Salt1: salt1, Salt2: salt2, G: srp.G, P: srp.PBytes(),
	})
	if err != nil {
		t.Fatalf("client Hash: %v", err)
	}
	return ans.A, ans.M1
}

// TestRoundTrip is the compatibility oracle: a verifier built by gotd's client
// must verify against a proof gotd's client computes for our challenge, and a
// wrong password must fail.
func TestRoundTrip(t *testing.T) {
	salt1Base, salt2 := newInput(t)
	verifier, salt1 := newVerifier(t, "correct horse", salt1Base, salt2)

	bPub, bSecret, err := srp.Challenge(verifier)
	if err != nil {
		t.Fatalf("Challenge: %v", err)
	}

	a, m1 := clientProof(t, "correct horse", salt1, salt2, bPub)
	if !srp.Verify(verifier, salt1, salt2, a, m1, bPub, bSecret) {
		t.Fatal("valid proof rejected")
	}

	aw, m1w := clientProof(t, "wrong password", salt1, salt2, bPub)
	if srp.Verify(verifier, salt1, salt2, aw, m1w, bPub, bSecret) {
		t.Fatal("wrong password accepted")
	}
}

// TestFreshChallengePerLogin confirms each challenge yields a distinct B and
// still verifies.
func TestFreshChallengePerLogin(t *testing.T) {
	salt1Base, salt2 := newInput(t)
	verifier, salt1 := newVerifier(t, "pw", salt1Base, salt2)

	b1Pub, b1Secret, err := srp.Challenge(verifier)
	if err != nil {
		t.Fatal(err)
	}
	b2Pub, _, err := srp.Challenge(verifier)
	if err != nil {
		t.Fatal(err)
	}
	if string(b1Pub) == string(b2Pub) {
		t.Fatal("challenge B not fresh")
	}
	a, m1 := clientProof(t, "pw", salt1, salt2, b1Pub)
	if !srp.Verify(verifier, salt1, salt2, a, m1, b1Pub, b1Secret) {
		t.Fatal("valid proof rejected")
	}
}

// TestDegenerateInputs rejects A ≡ 0 (mod p) and A = 0.
func TestDegenerateInputs(t *testing.T) {
	salt1Base, salt2 := newInput(t)
	verifier, salt1 := newVerifier(t, "pw", salt1Base, salt2)
	bPub, bSecret, err := srp.Challenge(verifier)
	if err != nil {
		t.Fatal(err)
	}

	if srp.Verify(verifier, salt1, salt2, srp.Pad(srp.Prime().Bytes()), make([]byte, 32), bPub, bSecret) {
		t.Fatal("A ≡ 0 mod p accepted")
	}
	if srp.Verify(verifier, salt1, salt2, srp.Pad(big.NewInt(0).Bytes()), make([]byte, 32), bPub, bSecret) {
		t.Fatal("A = 0 accepted")
	}
}

// TestPadExactness pins the padding to 256 bytes, low-order preserving.
func TestPadExactness(t *testing.T) {
	if got := srp.Pad([]byte{0x01}); len(got) != srp.PadLen || got[srp.PadLen-1] != 0x01 || got[0] != 0x00 {
		t.Fatalf("short pad wrong: len=%d", len(got))
	}
	// len 258: pad keeps the low 256 bytes, so over[2] lands at got[0].
	over := make([]byte, srp.PadLen+2)
	over[0], over[2] = 0xAA, 0xBB
	if got := srp.Pad(over); len(got) != srp.PadLen || got[0] != 0xBB {
		t.Fatalf("long pad wrong")
	}
}

// TestValidVerifier rejects degenerate verifiers (0, p) and accepts a real one.
func TestValidVerifier(t *testing.T) {
	if srp.ValidVerifier(srp.Pad(big.NewInt(0).Bytes())) {
		t.Fatal("v = 0 accepted")
	}
	if srp.ValidVerifier(srp.Pad(srp.Prime().Bytes())) {
		t.Fatal("v = p accepted")
	}
	salt1Base, salt2 := newInput(t)
	v, _ := newVerifier(t, "pw", salt1Base, salt2)
	if !srp.ValidVerifier(v) {
		t.Fatal("valid verifier rejected")
	}
}

// TestVerifyRejectsNoncanonicalSizes rejects an oversized A and a short M1
// rather than truncating them into the SRP math.
func TestVerifyRejectsNoncanonicalSizes(t *testing.T) {
	salt1Base, salt2 := newInput(t)
	verifier, salt1 := newVerifier(t, "pw", salt1Base, salt2)
	bPub, bSecret, err := srp.Challenge(verifier)
	if err != nil {
		t.Fatal(err)
	}
	a, m1 := clientProof(t, "pw", salt1, salt2, bPub)
	if !srp.Verify(verifier, salt1, salt2, a, m1, bPub, bSecret) {
		t.Fatal("valid proof rejected")
	}
	// A with 257 bytes (a leading byte prepended) must be refused.
	oversized := append([]byte{0x01}, a...)
	if srp.Verify(verifier, salt1, salt2, oversized, m1, bPub, bSecret) {
		t.Fatal("oversized A accepted")
	}
	// M1 shorter than SHA-256 must be refused.
	if srp.Verify(verifier, salt1, salt2, a, m1[:len(m1)-1], bPub, bSecret) {
		t.Fatal("short M1 accepted")
	}
}

// TestEmbeddedParams confirms the canonical group is valid for g=3.
func TestEmbeddedParams(t *testing.T) {
	if new(big.Int).Mod(srp.Prime(), big.NewInt(3)).Cmp(big.NewInt(2)) != 0 {
		t.Fatal("p mod 3 != 2; g=3 invalid for this prime")
	}
	if len(srp.PBytes()) != srp.PadLen {
		t.Fatalf("PBytes len = %d", len(srp.PBytes()))
	}
}
