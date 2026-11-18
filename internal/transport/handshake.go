package transport

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"time"
)

// The A3 handshake (see docs/rescope_v0.1.md). It proves the peer knows the
// secret; it does not encrypt or authenticate later frames.

const (
	helloMagic = "PSY1"
	nonceLen   = 32

	labelClient = "podsync/v1 client"
	labelServer = "podsync/v1 server"
)

func (t *TCP) mode() byte {
	if len(t.Secret) > 0 {
		return 1
	}
	return 0
}

func (t *TCP) checkSecret() error {
	if n := len(t.Secret); n > 0 && n < MinSecretLen {
		return ErrSecretTooShort
	}
	return nil
}

func macOf(secret []byte, label string, a, b []byte) []byte {
	m := hmac.New(sha256.New, secret)
	m.Write([]byte(label))
	m.Write(a)
	m.Write(b)
	return m.Sum(nil)
}

// exchangeHello sends our hello and checks the peer's.
func (t *TCP) exchangeHello(c net.Conn) error {
	if _, err := c.Write(append([]byte(helloMagic), t.mode())); err != nil {
		return err
	}
	var h [5]byte
	if _, err := io.ReadFull(c, h[:]); err != nil {
		return err
	}
	if string(h[:4]) != helloMagic {
		return ErrBadHello
	}
	if h[4] != t.mode() {
		return ErrModeMismatch
	}
	return nil
}

// handshake runs the handshake on c within timeout and clears the deadline
// on success. The caller closes c on error.
func (t *TCP) handshake(c net.Conn, dialer bool, timeout time.Duration) error {
	if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
		return err
	}
	if err := t.exchangeHello(c); err != nil {
		return err
	}
	if t.mode() == 1 {
		var err error
		if dialer {
			err = t.authAsDialer(c)
		} else {
			err = t.authAsListener(c)
		}
		if err != nil {
			return err
		}
	}
	return c.SetDeadline(time.Time{})
}

func (t *TCP) authAsListener(c net.Conn) error {
	ns := make([]byte, nonceLen)
	if _, err := rand.Read(ns); err != nil {
		return err
	}
	if _, err := c.Write(ns); err != nil {
		return err
	}
	resp := make([]byte, nonceLen+sha256.Size)
	if _, err := io.ReadFull(c, resp); err != nil {
		return err
	}
	nc, got := resp[:nonceLen], resp[nonceLen:]
	if !hmac.Equal(got, macOf(t.Secret, labelClient, ns, nc)) {
		return ErrAuth
	}
	_, err := c.Write(macOf(t.Secret, labelServer, nc, ns))
	return err
}

func (t *TCP) authAsDialer(c net.Conn) error {
	ns := make([]byte, nonceLen)
	if _, err := io.ReadFull(c, ns); err != nil {
		return err
	}
	nc := make([]byte, nonceLen)
	if _, err := rand.Read(nc); err != nil {
		return err
	}
	if _, err := c.Write(append(nc, macOf(t.Secret, labelClient, ns, nc)...)); err != nil {
		return err
	}
	got := make([]byte, sha256.Size)
	if _, err := io.ReadFull(c, got); err != nil {
		return err
	}
	if !hmac.Equal(got, macOf(t.Secret, labelServer, nc, ns)) {
		return fmt.Errorf("%w: bad server MAC", ErrAuth)
	}
	return nil
}
