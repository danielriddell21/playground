package zerotrust

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
)

// CA is a throwaway trust domain that issues x509 svids the way spire
// would, so mtls with spiffe ids works without running spire. With spire,
// workloadapi.NewX509Source gives the same Source and Bundle interfaces.
type CA struct {
	TD   spiffeid.TrustDomain
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
}

func NewCA(td string) (*CA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	trust := spiffeid.RequireTrustDomainFromString(td)
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: td + " ca"},
		URIs:                  []*url.URL{trust.ID().URL()},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, _ := x509.ParseCertificate(der)
	return &CA{TD: trust, cert: cert, key: key}, nil
}

func (ca *CA) Bundle() *x509bundle.Bundle {
	return x509bundle.FromX509Authorities(ca.TD, []*x509.Certificate{ca.cert})
}

// Issue makes an svid for spiffe://<td>/<workload>, valid for ttl.
func (ca *CA) Issue(workload string, ttl time.Duration) (*x509svid.SVID, error) {
	id, err := spiffeid.FromPath(ca.TD, "/ns/playground/sa/"+workload)
	if err != nil {
		return nil, err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		URIs:         []*url.URL{id.URL()},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(ttl),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, err
	}
	cert, _ := x509.ParseCertificate(der)
	return &x509svid.SVID{ID: id, Certificates: []*x509.Certificate{cert}, PrivateKey: key}, nil
}
