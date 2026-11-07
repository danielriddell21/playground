package xdsplay

import (
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func init() {
	set.Add("snapshot", "what envoy gets: listener, routes and clusters from one snapshot", snapshot)
	set.Add("rollout", "a fake envoy on ads acks each push while a canary rolls out and back", rollout)
}

var mesh = Mesh{ListenPort: 10000, API: "api:9000", Stable: "web:8080", Canary: "web-canary:8080"}

func snapshot(s *Session) {
	m := mesh
	m.CanaryPct = 25
	snap, err := m.Snapshot(1)
	if err != nil {
		s.Fail(err)
		return
	}
	s.Note("consistent: %v (every route's cluster exists, every listener's route exists)", snap.Consistent() == nil)
	var rows [][]any
	for _, r := range snap.GetResources(resource.ListenerType) {
		l := r.(*listener.Listener)
		rows = append(rows, []any{"listener", l.Name, fmt.Sprintf(":%d, routes over ads", l.Address.GetSocketAddress().GetPortValue())})
	}
	for _, r := range snap.GetResources(resource.RouteType) {
		for _, rt := range r.(*route.RouteConfiguration).VirtualHosts[0].Routes {
			to := rt.GetRoute().GetCluster()
			if wc := rt.GetRoute().GetWeightedClusters(); wc != nil {
				to = weights(wc)
			}
			rows = append(rows, []any{"route", rt.Match.GetPrefix(), to})
		}
	}
	for _, r := range snap.GetResources(resource.ClusterType) {
		c := r.(*cluster.Cluster)
		ep := c.LoadAssignment.Endpoints[0].LbEndpoints[0].GetEndpoint().Address.GetSocketAddress()
		proto := "http/1.1"
		if c.TypedExtensionProtocolOptions != nil {
			proto = "h2c"
		}
		rows = append(rows, []any{"cluster", c.Name, fmt.Sprintf("%s:%d %s", ep.Address, ep.GetPortValue(), proto)})
	}
	s.Table([]string{"kind", "name", "what"}, rows)
}

func rollout(s *Session) {
	if err := s.cp.Push(s.ctx, mesh); err != nil {
		s.Fail(err)
		return
	}
	conn, err := grpc.NewClient(s.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.Fail(err)
		return
	}
	defer conn.Close()
	stream, err := discovery.NewAggregatedDiscoveryServiceClient(conn).StreamAggregatedResources(s.ctx)
	if err != nil {
		s.Fail(err)
		return
	}
	node := &core.Node{Id: NodeID}
	sub := func(version, nonce string) error {
		return stream.Send(&discovery.DiscoveryRequest{Node: node, TypeUrl: resource.RouteType, ResourceNames: []string{routeName}, VersionInfo: version, ResponseNonce: nonce})
	}
	if err := sub("", ""); err != nil {
		s.Fail(err)
		return
	}

	// the canary fails its check at 50%
	var final atomic.Value
	var rolloutErr error
	go func() {
		rolloutErr = s.cp.Rollout(s.ctx, mesh, 300*time.Millisecond, func(pct uint32) error {
			if pct == 50 {
				return fmt.Errorf("canary 5xx rate 4.2%% over budget")
			}
			return nil
		})
		final.Store(strconv.Itoa(s.cp.Version()))
	}()

	var rows [][]any
	for {
		res, err := stream.Recv()
		if err != nil {
			s.Fail(err)
			return
		}
		var rc route.RouteConfiguration
		if err := res.Resources[0].UnmarshalTo(&rc); err != nil {
			s.Fail(err)
			return
		}
		for _, rt := range rc.VirtualHosts[0].Routes {
			if wc := rt.GetRoute().GetWeightedClusters(); wc != nil {
				rows = append(rows, []any{"v" + res.VersionInfo, weights(wc)})
			}
		}
		// ack like envoy does, or the next version never comes
		if err := sub(res.VersionInfo, res.Nonce); err != nil {
			s.Fail(err)
			return
		}
		// pushes made between acks coalesce, so stop on the last version
		// rather than counting responses
		if v, _ := final.Load().(string); v == res.VersionInfo {
			break
		}
	}
	s.Table([]string{"version", "web traffic"}, rows)
	s.Note("%v", rolloutErr)
}

func weights(wc *route.WeightedCluster) string {
	var parts []string
	for _, c := range wc.Clusters {
		parts = append(parts, fmt.Sprintf("%s=%d", c.Name, c.Weight.GetValue()))
	}
	return strings.Join(parts, " ")
}
