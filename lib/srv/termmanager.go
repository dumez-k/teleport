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

package srv

import (
	"sync"

	"github.com/gravitational/teleport/lib/utils"
	"github.com/gravitational/trace"
)

// TermManager handles the output stream of sessions.
// It deals with tasks like stream manipulation in order to enable message injection.
type TermManager struct {
	mu sync.Mutex
	W  *SwitchWriter
}

// NewTermManager creates a new TermManager.
func NewTermManager(w *SwitchWriter) *TermManager {
	return &TermManager{
		W: w,
	}
}

func (g *TermManager) Write(p []byte) (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	count, err := g.W.Write(p)
	if err != nil {
		return 0, trace.Wrap(err)
	}

	return count, nil
}

// BroadcastMessage injects a message into the stream.
func (g *TermManager) BroadcastMessage(message string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	data := []byte("\nTeleport > " + message + "\n")
	err := utils.WriteAll(g.W.WriteUnconditional, data)
	return trace.Wrap(err)
}
