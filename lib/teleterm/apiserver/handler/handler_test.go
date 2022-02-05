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

package handler_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	api "github.com/gravitational/teleport/lib/teleterm/api/protogen/golang/v1"
	"github.com/gravitational/teleport/lib/teleterm/apiserver/handler"
	"github.com/gravitational/teleport/lib/teleterm/clusters"
	"github.com/gravitational/teleport/lib/teleterm/daemon"
	"github.com/gravitational/teleport/lib/utils"

	"github.com/gravitational/trace"

	log "github.com/sirupsen/logrus"

	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	utils.InitLoggerForTests()
	log.SetFormatter(&trace.JSONFormatter{})
	os.Exit(m.Run())

}

func TestHandler(t *testing.T) {
	//utils.InitLoggerForTests()
	//	logger := utils.NewLoggerForTests()
	//	logger.SetFormatter(&trace.JSONFormatter{})

	logger := log.New()
	logger.ReplaceHooks(make(log.LevelHooks))
	logger.SetFormatter(&trace.TextFormatter{})
	logger.SetLevel(log.DebugLevel)
	logger.SetOutput(os.Stderr)

	storage, err := clusters.New(clusters.Config{
		Dir:                t.TempDir(),
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	d, err := daemon.New(daemon.Config{
		Storage:            storage,
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	h, err := handler.New(handler.Config{
		DaemonService: d,
	})
	require.NoError(t, err)

	cluster1, err := h.AddCluster(context.TODO(), &api.AddClusterRequest{
		Name: "localhost:4080",
	})
	require.NoError(t, err)
	require.False(t, cluster1.Connected)

	_, err = h.Login(context.TODO(), &api.LoginRequest{
		ClusterUri: "/clusters/localhost",
		Sso: &api.LoginRequest_SsoParams{
			ProviderType: "oidc",
			ProviderName: "google",
		},
	})

	require.NoError(t, err)

	leaves, err := h.DaemonService.ListLeafClusters(context.TODO(), "/clusters/localhost/leaves/tc")
	fmt.Print("KUBES: ", leaves)
	require.NoError(t, err)

	c1, err := h.DaemonService.GetCluster("/clusters/localhost/leaves/tc")
	fmt.Print("\n\n\n\n KUBES: ", c1)

	require.Error(t, err)
}

func FTestHandler(t *testing.T) {
	storage, err := clusters.New(clusters.Config{
		Dir:                t.TempDir(),
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	d, err := daemon.New(daemon.Config{
		Storage:            storage,
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	h, err := handler.New(handler.Config{
		DaemonService: d,
	})
	require.NoError(t, err)

	cluster1, err := h.AddCluster(context.TODO(), &api.AddClusterRequest{
		Name: "asteroid-moon.teleport.sh",
	})
	require.NoError(t, err)

	require.Equal(t, cluster1.Name, "asteroid-moon.teleport.sh")
	require.False(t, cluster1.Connected)

	response, err := h.ListRootClusters(context.TODO(), &api.ListClustersRequest{})
	require.NoError(t, err)
	require.Len(t, response.Clusters, 1)

	_, err = h.Login(context.TODO(), &api.LoginRequest{
		ClusterUri: "/clusters/asteroid-moon.teleport.sh",
		Sso: &api.LoginRequest_SsoParams{
			ProviderType: "github",
			ProviderName: "github",
		},
	})
	require.NoError(t, err)

	//kubes, err := h.DaemonService.ListKubes(context.TODO(), "/clusters/asteroid-moon.teleport.sh")
	//fmt.Print("KUBES: ", kubes)

	apps, err := h.DaemonService.ListApps(context.TODO(), "/clusters/asteroid-moon.teleport.sh")
	fmt.Print("KUBES: ", apps)

	require.NoError(t, err)

	//_, err = h.Logout(context.TODO(), &api.LogoutRequest{
	//	ClusterUri: "/clusters/localhost",
	//})
	//require.NoError(t, err)

	_, err = h.RemoveCluster(context.TODO(), &api.RemoveClusterRequest{
		ClusterUri: "/clusters/asteroid-moon.teleport.sh",
	})

	fmt.Print(trace.DebugReport(err))

	require.Error(t, err)

	//_, err = h.ListServers(context.TODO(), &api.ListServersRequest{
	//	ClusterUri: "/clusters/localhost",
	//})

}

func LocalTestHandler(t *testing.T) {
	storage, err := clusters.New(clusters.Config{
		Dir:                t.TempDir(),
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	d, err := daemon.New(daemon.Config{
		Storage:            storage,
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	h, err := handler.New(handler.Config{
		DaemonService: d,
	})
	require.NoError(t, err)

	cluster1, err := h.AddCluster(context.TODO(), &api.AddClusterRequest{
		Name: "localhost:4080",
	})
	require.NoError(t, err)

	require.Equal(t, cluster1.Name, "localhost")
	require.False(t, cluster1.Connected)

	response, err := h.ListRootClusters(context.TODO(), &api.ListClustersRequest{})
	require.NoError(t, err)
	require.Len(t, response.Clusters, 1)

	_, err = h.Login(context.TODO(), &api.LoginRequest{
		ClusterUri: "/clusters/localhost",
		Local: &api.LoginRequest_LocalParams{
			User:     "mama",
			Password: "123123",
		},
	})
	require.NoError(t, err)

	//_, err = h.Logout(context.TODO(), &api.LogoutRequest{
	//	ClusterUri: "/clusters/localhost",
	//})
	//require.NoError(t, err)

	_, err = h.RemoveCluster(context.TODO(), &api.RemoveClusterRequest{
		ClusterUri: "/clusters/localhost",
	})

	fmt.Print(trace.DebugReport(err))

	require.NoError(t, err)

	//_, err = h.ListServers(context.TODO(), &api.ListServersRequest{
	//	ClusterUri: "/clusters/localhost",
	//})

}
