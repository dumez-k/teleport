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

package teleterm

import (
	"fmt"
	"os"

	"github.com/gravitational/trace"
)

// ServerOpts contains configuration options for a Teleport Terminal service.
type Config struct {
	// Addr is the bind address for the server, either in "scheme://host:port"
	// format (allowing for "tcp", "unix", etc) or in "host:port" format (defaults
	// to "tcp").
	Addr string
	// ShutdownSignals is the set of captured signals that cause server shutdown.
	ShutdownSignals []os.Signal
	// HomeDir is the directory to store cluster profiles
	HomeDir string
	// InsecureSkipVerify is an option to skip HTTPS cert check
	InsecureSkipVerify bool
}

// CheckAndSetDefaults checks and sets default config values.
func (c *Config) CheckAndSetDefaults() error {
	if c.HomeDir == "" {
		return trace.BadParameter("missing home directory")
	}

	if c.Addr == "" {
		c.Addr = fmt.Sprintf("unix://%v/tshd.socket", c.HomeDir)
	}

	return nil
}

// String returns the config string representation
func (c *Config) String() string {
	return fmt.Sprintf("HomeDir=%+v, Addr=%s, Insecure=%+v", c.HomeDir, c.Addr, c.InsecureSkipVerify)
}
