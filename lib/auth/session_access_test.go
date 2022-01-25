/*
Copyright 2021 Gravitational, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package auth

import (
	"context"
	"testing"

	"github.com/gravitational/teleport/api/types"
	"github.com/stretchr/testify/require"
)

type startTestCase struct {
	host         types.Role
	sessionKind  types.SessionKind
	participants []SessionAccessContext
	expected     bool
}

func TestSessionAccessStart(t *testing.T) {
	testCases := []startTestCase{}

	for _, testCase := range testCases {
		evaluator := NewSessionAccessEvaluator([]types.Role{testCase.host}, testCase.sessionKind)
		result, _, err := evaluator.FulfilledFor(testCase.participants)
		require.NoError(t, err)
		require.Equal(t, testCase.expected, result)
	}
}

type joinTestCase struct {
	host        types.Role
	sessionKind types.SessionKind
	participant SessionAccessContext
	expected    bool
}

func successJoinTestCase(ctx context.Context) joinTestCase {
	_, role = CreateRole(context.TODO())
}

func TestSessionAccessJoin(t *testing.T) {
	testCases := []joinTestCase{}

	for _, testCase := range testCases {
		evaluator := NewSessionAccessEvaluator([]types.Role{testCase.host}, testCase.sessionKind)
		result, err := evaluator.CanJoin(testCase.participant)
		require.NoError(t, err)
		require.Equal(t, testCase.expected, result)
	}
}
