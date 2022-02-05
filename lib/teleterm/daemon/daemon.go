// Copyright 2021 Gravitational, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package daemon

import (
	"context"

	apiuri "github.com/gravitational/teleport/lib/teleterm/api/uri"
	"github.com/gravitational/teleport/lib/teleterm/clusters"
	"github.com/gravitational/teleport/lib/teleterm/gateway"

	"github.com/gravitational/trace"
)

type Cluster = clusters.Cluster
type Gateway = gateway.Gateway
type Database = clusters.Database
type Server = clusters.Server
type Kube = clusters.Kube
type App = clusters.App
type Leaf = clusters.LeafCluster
type CreateGatewayParams = clusters.CreateGatewayParams

// Start creates and starts a Teleport Terminal service.
func New(cfg Config) (*Service, error) {
	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}

	return &Service{
		Config: cfg,
	}, nil
}

// GetRootClusters returns a list of existing clusters
func (s *Service) GetRootClusters(ctx context.Context) ([]*Cluster, error) {
	clusters, err := s.Storage.ReadAll()
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return clusters, nil
}

func (s *Service) ListLeafClusters(ctx context.Context, uri string) ([]Leaf, error) {
	cluster, err := s.ResolveCluster(uri)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	// leaf cluster cannot have own leaves
	if cluster.URI.GetLeafCluster() != "" {
		return nil, nil
	}

	leaves, err := cluster.GetLeafClusters(ctx)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return leaves, nil
}

// AddCluster adds a cluster
func (s *Service) AddCluster(ctx context.Context, webProxyAddress string) (*Cluster, error) {
	cluster, err := s.Storage.Add(ctx, webProxyAddress)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return cluster, nil
}

// RemoveCluster removes cluster
func (s *Service) RemoveCluster(ctx context.Context, uri string) error {
	cluster, err := s.ResolveCluster(uri)
	if err != nil {
		return trace.Wrap(err)
	}

	if cluster.Connected() {
		if err := cluster.Logout(ctx); err != nil {
			return trace.Wrap(err)
		}
	}

	if err := s.Storage.Remove(ctx, cluster.Name); err != nil {
		return trace.Wrap(err)
	}

	return nil
}

// ResolveCluster returns a cluster by cluster resource URI
func (s *Service) ResolveCluster(uri string) (*Cluster, error) {
	clusterURI, err := apiuri.NewClusterFromResource(uri)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	cluster, err := s.Storage.GetByURI(clusterURI.String())
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return cluster, nil
}

// ClusterLogout logs a user out from the cluster
func (s *Service) ClusterLogout(ctx context.Context, uri string) error {
	cluster, err := s.ResolveCluster(uri)
	if err != nil {
		return trace.Wrap(err)
	}

	if err := cluster.Logout(ctx); err != nil {
		return trace.Wrap(err)
	}

	return nil
}

// CreateGateway creates a gateway to given targetURI
func (s *Service) CreateGateway(ctx context.Context, params CreateGatewayParams) (*Gateway, error) {
	cluster, err := s.ResolveCluster(params.TargetURI)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	gateway, err := cluster.CreateGateway(ctx, params)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	gateway.Open()

	s.gateways = append(s.gateways, gateway)

	return gateway, nil
}

// ListServers returns cluster servers
func (s *Service) ListServers(ctx context.Context, clusterURI string) ([]Server, error) {
	cluster, err := s.ResolveCluster(clusterURI)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	servers, err := cluster.GetServers(ctx)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return servers, nil
}

// ListServers returns cluster servers
func (s *Service) ListApps(ctx context.Context, clusterURI string) ([]App, error) {
	cluster, err := s.ResolveCluster(clusterURI)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	apps, err := cluster.GetApps(ctx)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return apps, nil
}

// RemoveGateway removes cluster gateway
func (s *Service) RemoveGateway(ctx context.Context, gatewayURI string) error {
	gateway, err := s.FindGateway(gatewayURI)
	if err != nil {
		return trace.Wrap(err)
	}

	gateway.Close()

	// remove closed gateway from list
	for index := range s.gateways {
		if s.gateways[index] == gateway {
			s.gateways = append(s.gateways[:index], s.gateways[index+1:]...)
			return nil
		}
	}

	return nil
}

// ListKubes lists k8s clusters
func (s *Service) ListKubes(ctx context.Context, uri string) ([]Kube, error) {
	cluster, err := s.ResolveCluster(uri)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	kubes, err := cluster.GetKubes(ctx)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return kubes, nil
}

// FindGateway finds a gateway by URI
func (s *Service) FindGateway(gatewayURI string) (*Gateway, error) {
	for _, gateway := range s.gateways {
		if gateway.URI.String() == gatewayURI {
			return gateway, nil
		}
	}

	return nil, trace.NotFound("gateway is not found: %v", gatewayURI)
}

// ListGateways lists gateways
func (s *Service) ListGateways(ctx context.Context) ([]*Gateway, error) {
	return s.gateways, nil
}

// Stop terminates all cluster open connections
func (s *Service) Stop() {
	for _, gateway := range s.gateways {
		gateway.Close()
	}
}

// Service is the cluster service
type Service struct {
	Config
	// gateways is the cluster gateways
	gateways []*gateway.Gateway
}
