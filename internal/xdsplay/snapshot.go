package xdsplay

import (
	"fmt"
	"net"
	"strconv"
	"time"

	cluster "github.com/envoyproxy/go-control-plane/envoy/config/cluster/v3"
	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	endpoint "github.com/envoyproxy/go-control-plane/envoy/config/endpoint/v3"
	listener "github.com/envoyproxy/go-control-plane/envoy/config/listener/v3"
	route "github.com/envoyproxy/go-control-plane/envoy/config/route/v3"
	routerv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/router/v3"
	hcm "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/network/http_connection_manager/v3"
	upstreamhttp "github.com/envoyproxy/go-control-plane/envoy/extensions/upstreams/http/v3"
	"github.com/envoyproxy/go-control-plane/pkg/cache/types"
	"github.com/envoyproxy/go-control-plane/pkg/cache/v3"
	"github.com/envoyproxy/go-control-plane/pkg/resource/v3"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	listenerName = "edge"
	routeName    = "edge"
)

// Mesh is the desired edge config. The control plane turns it into
// listeners, routes and clusters and pushes a new snapshot when it changes.
type Mesh struct {
	ListenPort uint32
	API        string // host:port of a grpc backend
	Stable     string // web, current version
	Canary     string // web, next version, may be empty
	CanaryPct  uint32 // 0-100 of web traffic sent to Canary
}

func (m Mesh) Snapshot(version int) (*cache.Snapshot, error) {
	clusters := []types.Resource{
		h2Cluster("api", m.API),
		h1Cluster("web-stable", m.Stable),
	}
	weights := []*route.WeightedCluster_ClusterWeight{{Name: "web-stable", Weight: wrapperspb.UInt32(100 - m.CanaryPct)}}
	if m.Canary != "" && m.CanaryPct > 0 {
		clusters = append(clusters, h1Cluster("web-canary", m.Canary))
		weights = append(weights, &route.WeightedCluster_ClusterWeight{Name: "web-canary", Weight: wrapperspb.UInt32(m.CanaryPct)})
	} else {
		weights[0].Weight = wrapperspb.UInt32(100)
	}
	l, err := httpListener(m.ListenPort)
	if err != nil {
		return nil, err
	}
	return cache.NewSnapshot(strconv.Itoa(version), map[resource.Type][]types.Resource{
		resource.ClusterType:  clusters,
		resource.RouteType:    {routes(weights)},
		resource.ListenerType: {l},
	})
}

func routes(weights []*route.WeightedCluster_ClusterWeight) *route.RouteConfiguration {
	to := func(prefix, cl string) *route.Route {
		return &route.Route{
			Match:  &route.RouteMatch{PathSpecifier: &route.RouteMatch_Prefix{Prefix: prefix}},
			Action: &route.Route_Route{Route: &route.RouteAction{ClusterSpecifier: &route.RouteAction_Cluster{Cluster: cl}}},
		}
	}
	return &route.RouteConfiguration{
		Name: routeName,
		VirtualHosts: []*route.VirtualHost{{
			Name:    "edge",
			Domains: []string{"*"},
			Routes: []*route.Route{
				to("/api.v1.", "api"),
				{
					Match: &route.RouteMatch{PathSpecifier: &route.RouteMatch_Prefix{Prefix: "/"}},
					Action: &route.Route_Route{Route: &route.RouteAction{
						ClusterSpecifier: &route.RouteAction_WeightedClusters{WeightedClusters: &route.WeightedCluster{Clusters: weights}},
						RetryPolicy: &route.RetryPolicy{
							RetryOn:    "connect-failure,refused-stream,unavailable",
							NumRetries: wrapperspb.UInt32(2),
						},
					}},
				},
			},
		}},
	}
}

func baseCluster(name, addr string) *cluster.Cluster {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		host, port = addr, "80"
	}
	p, _ := strconv.Atoi(port)
	return &cluster.Cluster{
		Name:                 name,
		ConnectTimeout:       durationpb.New(time.Second),
		ClusterDiscoveryType: &cluster.Cluster_Type{Type: cluster.Cluster_STRICT_DNS},
		LbPolicy:             cluster.Cluster_ROUND_ROBIN,
		LoadAssignment: &endpoint.ClusterLoadAssignment{
			ClusterName: name,
			Endpoints: []*endpoint.LocalityLbEndpoints{{
				LbEndpoints: []*endpoint.LbEndpoint{{
					HostIdentifier: &endpoint.LbEndpoint_Endpoint{Endpoint: &endpoint.Endpoint{
						Address: &core.Address{Address: &core.Address_SocketAddress{SocketAddress: &core.SocketAddress{
							Address:       host,
							PortSpecifier: &core.SocketAddress_PortValue{PortValue: uint32(p)},
						}}},
					}},
				}},
			}},
		},
		OutlierDetection: &cluster.OutlierDetection{
			Consecutive_5Xx:  wrapperspb.UInt32(5),
			BaseEjectionTime: durationpb.New(10 * time.Second),
		},
	}
}

func h1Cluster(name, addr string) *cluster.Cluster { return baseCluster(name, addr) }

// h2Cluster speaks h2c upstream so grpc clients going through envoy work.
func h2Cluster(name, addr string) *cluster.Cluster {
	c := baseCluster(name, addr)
	opts, _ := anypb.New(&upstreamhttp.HttpProtocolOptions{
		UpstreamProtocolOptions: &upstreamhttp.HttpProtocolOptions_ExplicitHttpConfig_{
			ExplicitHttpConfig: &upstreamhttp.HttpProtocolOptions_ExplicitHttpConfig{
				ProtocolConfig: &upstreamhttp.HttpProtocolOptions_ExplicitHttpConfig_Http2ProtocolOptions{Http2ProtocolOptions: &core.Http2ProtocolOptions{}},
			},
		},
	})
	c.TypedExtensionProtocolOptions = map[string]*anypb.Any{"envoy.extensions.upstreams.http.v3.HttpProtocolOptions": opts}
	return c
}

func httpListener(port uint32) (*listener.Listener, error) {
	router, err := anypb.New(&routerv3.Router{})
	if err != nil {
		return nil, err
	}
	mgr, err := anypb.New(&hcm.HttpConnectionManager{
		StatPrefix: "edge",
		CodecType:  hcm.HttpConnectionManager_AUTO,
		RouteSpecifier: &hcm.HttpConnectionManager_Rds{Rds: &hcm.Rds{
			RouteConfigName: routeName,
			ConfigSource: &core.ConfigSource{
				ResourceApiVersion:    core.ApiVersion_V3,
				ConfigSourceSpecifier: &core.ConfigSource_Ads{Ads: &core.AggregatedConfigSource{}},
			},
		}},
		HttpFilters: []*hcm.HttpFilter{{Name: "envoy.filters.http.router", ConfigType: &hcm.HttpFilter_TypedConfig{TypedConfig: router}}},
	})
	if err != nil {
		return nil, err
	}
	return &listener.Listener{
		Name: listenerName,
		Address: &core.Address{Address: &core.Address_SocketAddress{SocketAddress: &core.SocketAddress{
			Address:       "0.0.0.0",
			PortSpecifier: &core.SocketAddress_PortValue{PortValue: port},
		}}},
		FilterChains: []*listener.FilterChain{{
			Filters: []*listener.Filter{{Name: "envoy.filters.network.http_connection_manager", ConfigType: &listener.Filter_TypedConfig{TypedConfig: mgr}}},
		}},
	}, nil
}

func describe(m Mesh) string {
	return fmt.Sprintf("stable=%s canary=%s %d%%", m.Stable, m.Canary, m.CanaryPct)
}
