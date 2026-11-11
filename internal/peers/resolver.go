// Package peers resolves the set of peer addresses from DNS (or a static
// list) instead of running gossip membership.
package peers

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
)

// Resolver returns the current "host:port" peer list, self included.
type Resolver interface {
	Resolve(ctx context.Context) ([]string, error)
}

type static []string

// Static returns a Resolver that always returns addrs.
func Static(addrs ...string) Resolver { return static(slices.Clone(addrs)) }

func (s static) Resolve(context.Context) ([]string, error) { return slices.Clone(s), nil }

type ipLookuper interface {
	LookupIPAddr(ctx context.Context, host string) ([]net.IPAddr, error)
}

type dnsResolver struct {
	service, namespace string
	port               int
	lookup             ipLookuper
}

// DNS resolves <service>.<namespace>.svc.cluster.local and returns ip:port for
// each A/AAAA record, sorted. An optional *net.Resolver replaces
// net.DefaultResolver (for tests).
func DNS(service, namespace string, port int, r ...*net.Resolver) Resolver {
	var l ipLookuper = net.DefaultResolver
	if len(r) > 0 && r[0] != nil {
		l = r[0]
	}
	return dnsResolver{service: service, namespace: namespace, port: port, lookup: l}
}

func (d dnsResolver) Resolve(ctx context.Context) ([]string, error) {
	host := fmt.Sprintf("%s.%s.svc.cluster.local", d.service, d.namespace)
	ips, err := d.lookup.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(ips))
	for _, ip := range ips {
		out = append(out, net.JoinHostPort(ip.IP.String(), strconv.Itoa(d.port)))
	}
	slices.Sort(out)
	return out, nil
}
