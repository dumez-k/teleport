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

package daemon_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/gravitational/teleport/lib/teleterm/daemon"
	"github.com/gravitational/trace"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const (
	profileDir = "/tmp/tshd/data"
)

func GTestStart(t *testing.T) {
	d, err := daemon.New(daemon.Config{
		Dir:                profileDir,
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	err = d.Init()
	require.NoError(t, err)

	//fmt.Printf("\n\n\n ALL CLUSTER: %+v", d.GetClusters())

	_, err = d.CreateCluster(context.TODO(), "localhost:4080")
	//fmt.Printf("CLUSTER: %+v", d.GetClusters()[0])
	require.Error(t, err)

	//cluster, err := d.GetCluster("/clusters/localhost")
	//servers, err := cluster.GetServers(context.TODO())
	//fmt.Print("AAAAAAAAAAAAAAAAAAAAAA:", servers)
	//require.NoError(t, err)

	//err = cluster.LocalLogin(context.TODO(), "papa", "123123", "")
	//require.NoError(t, err)

	//cluster.get

	//cluster.CreateGateway(context.TODO(), )

	//_, err = d.GetCluster("localhost:3080")
	//require.NoError(t, err)

	//cluster.SSOLogin(context.Background(), "github", "github")

	//roles, _ := cluster.GetRoles(context.TODO())
	//fmt.Print("AAAAAAAAAAAAAAAAAAAAAA:", roles)
	//require.Error(t, err)

	//fmt.Printf("AAAAAAAAAAAAAAAAAAAAAAAA: %+v \n", d.GetClusters()[0])

	//dbs, err := cluster.GetDatabases(context.TODO())
	//fmt.Printf("CLUSTER OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO: %+v", dbs[0].URI)
	//require.NoError(t, err)

	// gateway, err := cluster.CreateGateway(context.TODO(), dbs[0].URI, "1223", "")
	// fmt.Printf("\n\n MAMAMAMAMACLUSTER: %+v", gateway)
	// require.NoError(t, err)

	//fmt.Print("\n\n\nZOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOPA", uri.Parse(gateway.URI).Cluster())
	// err = d.RemoveGateway(context.TODO(), gateway.URI)
	// fmt.Printf("CLUSTER: %+v", gateway)
	// require.NoError(t, err)

	//gateways := cluster.GetGateways()
	//require.Len(t, gateways, 0)
	//require.NoError(t, err)

	//time.Sleep(20 * time.Second)
	// 	err = cluster2.SSOLogin(context.TODO(), "saml", "okta")
	// 	require.NoError(t, err)
}

func FTestS(t *testing.T) {
	log.SetFormatter(&trace.TextFormatter{})
	log.SetLevel(log.DebugLevel)
	d, err := daemon.New(daemon.Config{
		Dir:                profileDir,
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	err = d.Init()
	require.NoError(t, err)

	//cluster, err := d.GetCluster("/clusters/localhost")
	//require.NoError(t, err)

	cluster, err := d.AddCluster(context.TODO(), "localhost:4080")
	require.NoError(t, err)

	err = cluster.LocalLogin(context.TODO(), "papa", "123123", "")
	require.NoError(t, err)
}

func TestS(t *testing.T) {
	d, err := daemon.New(daemon.Config{
		Dir:                t.TempDir(),
		InsecureSkipVerify: true,
	})
	require.NoError(t, err)

	err = d.Init()
	require.NoError(t, err)

	cluster, err := d.AddCluster(context.TODO(), "localhost:4080")
	require.NoError(t, err)

	cluster, err = d.GetCluster("/clusters/localhost")
	require.NoError(t, err)

	//	err = cluster.LocalLogin(context.TODO(), "papa", "123123", "fd")
	//	require.Error(t, err)

	err = cluster.SSOLogin(context.Background(), "oidc", "google")
	require.NoError(t, err)

	fmt.Println("ISCONNECTED: ", cluster.Connected())

	//dbs, err := cluster.GetDatabases(context.TODO())
	//fmt.Print("AAAAAAAAAAAAAA:", dbs)
	//require.NoError(t, err)

	//_, err = cluster.SyncAuthPreference(context.TODO())
}
