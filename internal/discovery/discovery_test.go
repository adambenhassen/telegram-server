package discovery_test

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/discovery"
)

func TestDocumentIsCanonicalAndContainsOnlyPublicIdentity(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	doc, err := discovery.NewDocument("mtproto.example.com:443", 2, &key.PublicKey)
	if err != nil {
		t.Fatalf("new document: %v", err)
	}
	first, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal document: %v", err)
	}
	second, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal document a second time: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("document is not deterministic:\n%s\n%s", first, second)
	}
	if string(first) != `{"version":1,"mtproto":{"endpoint":"mtproto.example.com:443","dc_id":2,"rsa_spki":"`+doc.MTProto.RSASPKI+`"}}` {
		t.Fatalf("document JSON = %s", first)
	}
	if bytes.Contains(first, []byte("private")) {
		t.Fatalf("document contains private-key material: %s", first)
	}
	der, err := base64.StdEncoding.DecodeString(doc.MTProto.RSASPKI)
	if err != nil {
		t.Fatalf("decode SPKI: %v", err)
	}
	parsed, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		t.Fatalf("parse SPKI: %v", err)
	}
	if got, ok := parsed.(*rsa.PublicKey); !ok || got.N.Cmp(key.N) != 0 || got.E != key.E {
		t.Fatal("document SPKI does not identify the loaded public key")
	}
}

func TestPreflightRequestHasExactWireShape(t *testing.T) {
	t.Parallel()

	var nonce [discovery.NonceSize]byte
	for i := range nonce {
		nonce[i] = byte(i + 1)
	}
	request, err := discovery.BuildPreflightRequest(nonce[:])
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	want := append([]byte(discovery.RequestMagic), nonce[:]...)
	if !bytes.Equal(request, want) {
		t.Fatalf("request = %x, want %x", request, want)
	}
}

func TestPreflightResponseHasExactWireShape(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	var nonce [discovery.NonceSize]byte
	for i := range nonce {
		nonce[i] = byte(i + 1)
	}
	response, err := discovery.BuildPreflightResponse(2, &key.PublicKey, nonce[:])
	if err != nil {
		t.Fatalf("build response: %v", err)
	}
	if !bytes.Equal(response[:discovery.ResponseMagicSize], []byte(discovery.ResponseMagic)) {
		t.Fatalf("response magic = %q", response[:discovery.ResponseMagicSize])
	}
	if !bytes.Equal(response[discovery.ResponseMagicSize:discovery.ResponseMagicSize+discovery.NonceSize], nonce[:]) {
		t.Fatal("response nonce is not echoed")
	}
	bodyLenOffset := discovery.ResponseMagicSize + discovery.NonceSize
	bodyLen := binary.BigEndian.Uint32(response[bodyLenOffset : bodyLenOffset+4])
	spkiLenOffset := bodyLenOffset + 4 + 4
	spkiLen := binary.BigEndian.Uint16(response[spkiLenOffset : spkiLenOffset+2])
	if int(bodyLen) != 6+int(spkiLen) {
		t.Fatalf("body length = %d, SPKI length = %d", bodyLen, spkiLen)
	}
	if len(response) != spkiLenOffset+2+int(spkiLen) {
		t.Fatalf("response length = %d, want %d", len(response), spkiLenOffset+2+int(spkiLen))
	}
}

func TestParsePreflightResponseRejectsLengthBoundaries(t *testing.T) {
	t.Parallel()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	var nonce [discovery.NonceSize]byte
	response, err := discovery.BuildPreflightResponse(2, &key.PublicKey, nonce[:])
	if err != nil {
		t.Fatalf("build response: %v", err)
	}
	bodyLenOffset := discovery.ResponseMagicSize + discovery.NonceSize
	spkiLenOffset := bodyLenOffset + 4 + 4
	bodyLen := binary.BigEndian.Uint32(response[bodyLenOffset : bodyLenOffset+4])
	cases := map[string]func([]byte){
		"body below minimum": func(raw []byte) {
			binary.BigEndian.PutUint32(raw[bodyLenOffset:bodyLenOffset+4], 5)
		},
		"body above maximum": func(raw []byte) {
			binary.BigEndian.PutUint32(raw[bodyLenOffset:bodyLenOffset+4], discovery.MaxSPKILength+7)
		},
		"body truncated": func(raw []byte) {
			binary.BigEndian.PutUint32(raw[bodyLenOffset:bodyLenOffset+4], bodyLen-1)
		},
		"body overlong": func(raw []byte) {
			binary.BigEndian.PutUint32(raw[bodyLenOffset:bodyLenOffset+4], bodyLen+1)
		},
		"SPKI zero": func(raw []byte) {
			binary.BigEndian.PutUint16(raw[spkiLenOffset:spkiLenOffset+2], 0)
		},
		"SPKI above maximum": func(raw []byte) {
			binary.BigEndian.PutUint16(raw[spkiLenOffset:spkiLenOffset+2], discovery.MaxSPKILength+1)
		},
		"trailing bytes":     func(raw []byte) {},
		"truncated response": func(raw []byte) {},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			raw := bytes.Clone(response)
			switch name {
			case "trailing bytes":
				raw = append(raw, 1)
			case "truncated response":
				raw = raw[:len(raw)-1]
			default:
				mutate(raw)
			}
			if _, _, err := discovery.ParsePreflightResponse(raw, nonce[:]); err == nil {
				t.Fatal("malformed response was accepted")
			}
		})
	}
}
