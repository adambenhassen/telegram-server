// Package discovery contains the public telegramd enrollment wire contract.
// It has no database or server dependencies so the client-config command can
// render the document before the service resources are initialized.
package discovery

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
)

const (
	// Version is the version of both the static document and the local
	// same-endpoint preflight protocol.
	Version = 1
	// RequestMagic and ResponseMagic are deliberately fixed-width ASCII tags.
	RequestMagic  = "telegramd-key-v1"
	ResponseMagic = "telegramd-key-r1"
	// NonceSize bounds the opaque request nonce and prevents response replay.
	NonceSize = 32
	// MaxSPKILength is the maximum DER SubjectPublicKeyInfo carried by a
	// preflight response. A 2048-bit RSA key is substantially smaller.
	MaxSPKILength = 4096

	RequestMagicSize  = len(RequestMagic)
	ResponseMagicSize = len(ResponseMagic)
)

// Document is the canonical v1 public discovery document.
type Document struct {
	Version int            `json:"version"`
	MTProto MTProtoBinding `json:"mtproto"`
}

// MTProtoBinding is the endpoint, DC and public RSA identity a server uses.
type MTProtoBinding struct {
	Endpoint string `json:"endpoint"`
	DCID     int    `json:"dc_id"`
	RSASPKI  string `json:"rsa_spki"`
}

// NewDocument constructs the canonical public document from the exact
// endpoint, DC and key used by the server.
func NewDocument(endpoint string, dcID int, pub *rsa.PublicKey) (Document, error) {
	if err := ValidateEndpoint(endpoint); err != nil {
		return Document{}, fmt.Errorf("validate advertised endpoint: %w", err)
	}
	if err := ValidateDCID(dcID); err != nil {
		return Document{}, err
	}
	der, err := MarshalSPKI(pub)
	if err != nil {
		return Document{}, err
	}
	return Document{
		Version: Version,
		MTProto: MTProtoBinding{
			Endpoint: endpoint,
			DCID:     dcID,
			RSASPKI:  base64.StdEncoding.EncodeToString(der),
		},
	}, nil
}

// MarshalDocument returns one deterministic compact JSON document. Encoding
// json struct fields in declaration order is intentional: this output is a
// static artifact, so the same key and configuration must produce identical
// bytes across invocations.
func MarshalDocument(doc Document) ([]byte, error) {
	if doc.Version != Version {
		return nil, fmt.Errorf("unsupported discovery document version %d", doc.Version)
	}
	if err := ValidateEndpoint(doc.MTProto.Endpoint); err != nil {
		return nil, fmt.Errorf("validate document endpoint: %w", err)
	}
	if err := ValidateDCID(doc.MTProto.DCID); err != nil {
		return nil, err
	}
	der, err := base64.StdEncoding.DecodeString(doc.MTProto.RSASPKI)
	if err != nil {
		return nil, fmt.Errorf("decode document SPKI: %w", err)
	}
	if base64.StdEncoding.EncodeToString(der) != doc.MTProto.RSASPKI {
		return nil, errors.New("document SPKI is not canonical base64")
	}
	if _, err := ParseSPKI(der); err != nil {
		return nil, err
	}
	return json.Marshal(doc)
}

// WriteDocument writes a complete canonical document. The document is
// marshaled before touching the writer, so validation or encoding failures
// cannot produce a partial success result.
func WriteDocument(w io.Writer, doc Document) error {
	data, err := MarshalDocument(doc)
	if err != nil {
		return err
	}
	n, err := w.Write(data)
	if err != nil {
		return fmt.Errorf("write discovery document: %w", err)
	}
	if n != len(data) {
		return fmt.Errorf("write discovery document: %w", io.ErrShortWrite)
	}
	return nil
}

// BuildPreflightRequest builds the exact local-direct request.
func BuildPreflightRequest(nonce []byte) ([]byte, error) {
	if len(nonce) != NonceSize {
		return nil, fmt.Errorf("preflight nonce is %d bytes, want %d", len(nonce), NonceSize)
	}
	request := make([]byte, RequestMagicSize+NonceSize)
	copy(request, RequestMagic)
	copy(request[RequestMagicSize:], nonce)
	return request, nil
}

// BuildPreflightResponse builds the bounded local-direct response.
func BuildPreflightResponse(dcID int, pub *rsa.PublicKey, nonce []byte) ([]byte, error) {
	if len(nonce) != NonceSize {
		return nil, fmt.Errorf("preflight nonce is %d bytes, want %d", len(nonce), NonceSize)
	}
	if err := ValidateDCID(dcID); err != nil {
		return nil, err
	}
	der, err := MarshalSPKI(pub)
	if err != nil {
		return nil, err
	}
	if len(der) < 1 || len(der) > MaxSPKILength {
		return nil, fmt.Errorf("SPKI length is %d, want 1..%d", len(der), MaxSPKILength)
	}
	bodyLen := 6 + len(der)
	response := make([]byte, ResponseMagicSize+NonceSize+4+bodyLen)
	copy(response, ResponseMagic)
	copy(response[ResponseMagicSize:], nonce)
	offset := ResponseMagicSize + NonceSize
	binary.BigEndian.PutUint32(response[offset:offset+4], uint32(bodyLen)) // #nosec G115 -- bodyLen is bounded by MaxSPKILength.
	offset += 4
	binary.BigEndian.PutUint32(response[offset:offset+4], uint32(int32(dcID))) // #nosec G115 -- ValidateDCID bounds the intentional signed wire conversion.
	offset += 4
	binary.BigEndian.PutUint16(response[offset:offset+2], uint16(len(der))) // #nosec G115 -- DER length is bounded by MaxSPKILength.
	offset += 2
	copy(response[offset:], der)
	return response, nil
}

// ParsePreflightResponse strictly parses one response and rejects trailing
// bytes, malformed lengths and non-RSA public keys.
func ParsePreflightResponse(response, nonce []byte) (int, *rsa.PublicKey, error) {
	minimum := ResponseMagicSize + NonceSize + 4 + 6
	if len(response) < minimum {
		return 0, nil, fmt.Errorf("preflight response is %d bytes, want at least %d", len(response), minimum)
	}
	if string(response[:ResponseMagicSize]) != ResponseMagic {
		return 0, nil, errors.New("invalid preflight response magic")
	}
	if len(nonce) != NonceSize || !equalBytes(response[ResponseMagicSize:ResponseMagicSize+NonceSize], nonce) {
		return 0, nil, errors.New("preflight response nonce does not match request")
	}
	offset := ResponseMagicSize + NonceSize
	bodyLen := binary.BigEndian.Uint32(response[offset : offset+4])
	offset += 4
	if bodyLen < 6 || uint64(bodyLen) > uint64(MaxSPKILength+6) {
		return 0, nil, fmt.Errorf("invalid preflight body length %d", bodyLen)
	}
	if uint64(len(response)-offset) != uint64(bodyLen) { // #nosec G115 -- offset is below len(response), and bodyLen is a bounded wire length.
		return 0, nil, errors.New("preflight response has truncated or trailing body")
	}
	dcID := int(int32(binary.BigEndian.Uint32(response[offset : offset+4]))) // #nosec G115 -- the wire field is intentionally decoded as signed int32.
	offset += 4
	if err := ValidateDCID(dcID); err != nil {
		return 0, nil, err
	}
	spkiLen := int(binary.BigEndian.Uint16(response[offset : offset+2]))
	offset += 2
	if spkiLen < 1 || spkiLen > MaxSPKILength || bodyLen != uint32(6+spkiLen) {
		return 0, nil, fmt.Errorf("invalid preflight SPKI length %d", spkiLen)
	}
	pub, err := ParseSPKI(response[offset : offset+spkiLen])
	if err != nil {
		return 0, nil, err
	}
	return dcID, pub, nil
}

// MarshalSPKI returns a validated DER SubjectPublicKeyInfo for pub.
func MarshalSPKI(pub *rsa.PublicKey) ([]byte, error) {
	if err := ValidatePublicKey(pub); err != nil {
		return nil, err
	}
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("marshal SubjectPublicKeyInfo: %w", err)
	}
	return der, nil
}

// ParseSPKI parses and validates one 2048-bit RSA SubjectPublicKeyInfo.
func ParseSPKI(der []byte) (*rsa.PublicKey, error) {
	if len(der) < 1 || len(der) > MaxSPKILength {
		return nil, fmt.Errorf("SPKI length is %d, want 1..%d", len(der), MaxSPKILength)
	}
	key, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, fmt.Errorf("parse SubjectPublicKeyInfo: %w", err)
	}
	pub, ok := key.(*rsa.PublicKey)
	if !ok {
		return nil, errors.New("discovery key is not RSA")
	}
	if err := ValidatePublicKey(pub); err != nil {
		return nil, err
	}
	canonical, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, fmt.Errorf("marshal parsed SubjectPublicKeyInfo: %w", err)
	}
	if !equalBytes(canonical, der) {
		return nil, errors.New("SubjectPublicKeyInfo is not canonical DER")
	}
	return pub, nil
}

// ValidatePublicKey enforces the RSA identity accepted by the MTProto server.
func ValidatePublicKey(pub *rsa.PublicKey) error {
	if pub == nil || pub.N == nil {
		return errors.New("RSA public key is nil")
	}
	if pub.N.Sign() <= 0 || pub.N.BitLen() != 2048 || pub.N.Bit(0) == 0 {
		return errors.New("RSA public key must have a positive odd 2048-bit modulus")
	}
	if pub.E < 3 || pub.E%2 == 0 || pub.E > math.MaxInt32 {
		return errors.New("RSA public key exponent must be an odd value at least 3")
	}
	return nil
}

// ValidateDCID validates the signed integer carried on the preflight wire.
func ValidateDCID(dcID int) error {
	if dcID <= 0 || int64(dcID) > math.MaxInt32 {
		return fmt.Errorf("DC id %d must be positive and fit int32", dcID)
	}
	return nil
}

// ValidateEndpoint validates the host:port shape used by the document. It
// intentionally does not resolve the host: an advertised private or loopback
// endpoint is valid for local deployments, while resolution belongs to the
// client route selected from the user's input.
func ValidateEndpoint(endpoint string) error {
	if endpoint == "" || strings.TrimSpace(endpoint) != endpoint {
		return errors.New("endpoint must be host:port")
	}
	host, portText, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" {
		return errors.New("endpoint must be host:port with a non-empty host")
	}
	if strings.ContainsAny(host, "\r\n\t /?#@%") {
		return errors.New("endpoint host contains invalid characters")
	}
	if portText == "" || strings.Trim(portText, "0123456789") != "" {
		return errors.New("endpoint port must be an integer")
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		return errors.New("endpoint port must be an integer")
	}
	if port < 1 || port > 65535 {
		return errors.New("endpoint port must be between 1 and 65535")
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsUnspecified() || ip.IsMulticast() {
			return errors.New("endpoint IP must not be unspecified or multicast")
		}
		return nil
	}
	if len(host) > 253 {
		return errors.New("endpoint DNS name exceeds 253 octets")
	}
	for label := range strings.SplitSeq(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return errors.New("endpoint DNS labels must be 1..63 characters and not start or end with a hyphen")
		}
		for _, ch := range label {
			if (ch < 'a' || ch > 'z') && (ch < 'A' || ch > 'Z') && (ch < '0' || ch > '9') && ch != '-' {
				return errors.New("endpoint DNS name contains invalid characters")
			}
		}
	}
	return nil
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
