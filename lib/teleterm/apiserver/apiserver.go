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

package apiserver

import (
	"net"
	"strings"

	"github.com/gravitational/teleport/lib/teleterm/apiserver/handler"
	"github.com/gravitational/trace"
	"google.golang.org/grpc"

	api "github.com/gravitational/teleport/lib/teleterm/api/protogen/golang/v1"
)

func New(cfg Config) (apiServer *APIServer, err error) {
	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}

	serviceHandler, err := handler.New(
		handler.Config{
			DaemonService: cfg.Daemon,
		},
	)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	ls, err := newListener(cfg.HostAddr)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	grpcServer := grpc.NewServer(grpc.Creds(nil), grpc.ChainUnaryInterceptor(
		withErrorHandling(cfg.Log),
	))

	api.RegisterTerminalServiceServer(grpcServer, serviceHandler)

	return &APIServer{cfg, ls, grpcServer}, nil
}

// ServeAndWait starts the server goroutine and waits until it exits.
func (s *APIServer) Serve() error {
	return s.grpcServer.Serve(s.ls)
}

// Close terminates the server and closes all open connections
func (s *APIServer) Stop() {
	s.grpcServer.GracefulStop()
}

func newListener(host string) (net.Listener, error) {
	var network, addr string

	parts := strings.SplitN(host, "://", 2)
	network = parts[0]
	switch network {
	case "unix":
		addr = parts[1]
	default:
		return nil, trace.BadParameter("invalid unix socket address: %s", network)
	}

	lis, err := net.Listen(network, addr)
	if err != nil {
		return nil, trace.Wrap(err)
	}

	return lis, nil
}

// Server is a combination of the underlying grpc.Server and its RuntimeOpts.
type APIServer struct {
	Config
	// ls is the server listener
	ls net.Listener
	// grpc is an instance of grpc server
	grpcServer *grpc.Server
}
