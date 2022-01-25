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

func successStartTestCase(t *testing.T) startTestCase {
	srv := newTestTLSServer(t)
	_, hostRole, err := CreateUserAndRole(srv.Auth(), "host", nil)
	require.NoError(t, err)
	_, participantRole, err := CreateUserAndRole(srv.Auth(), "participant", nil)
	require.NoError(t, err)

	hostRole.SetSessionRequirePolicies([]*types.SessionRequirePolicy{{
		Filter: "contains(participant.roles, \"user:participant\")",
		Kinds:  []string{string(types.SSHSessionKind)},
		Count:  2,
	}})

	return startTestCase{
		host:        hostRole,
		sessionKind: types.SSHSessionKind,
		participants: []SessionAccessContext{
			{
				Username: "participant",
				Roles:    []types.Role{participantRole},
			},
			{
				Username: "participant2",
				Roles:    []types.Role{participantRole},
			},
		},
		expected: true,
	}
}

func failCountStartTestCase(t *testing.T) startTestCase {
	srv := newTestTLSServer(t)
	_, hostRole, err := CreateUserAndRole(srv.Auth(), "host", nil)
	require.NoError(t, err)
	_, participantRole, err := CreateUserAndRole(srv.Auth(), "participant", nil)
	require.NoError(t, err)

	hostRole.SetSessionRequirePolicies([]*types.SessionRequirePolicy{{
		Filter: "contains(participant.roles, \"user:participant\")",
		Kinds:  []string{string(types.SSHSessionKind)},
		Count:  3,
	}})

	return startTestCase{
		host:        hostRole,
		sessionKind: types.SSHSessionKind,
		participants: []SessionAccessContext{
			{
				Username: "participant",
				Roles:    []types.Role{participantRole},
			},
			{
				Username: "participant2",
				Roles:    []types.Role{participantRole},
			},
		},
		expected: false,
	}
}

func failFilterStartTestCase(t *testing.T) startTestCase {
	srv := newTestTLSServer(t)
	_, hostRole, err := CreateUserAndRole(srv.Auth(), "host", nil)
	require.NoError(t, err)
	_, participantRole, err := CreateUserAndRole(srv.Auth(), "participant", nil)
	require.NoError(t, err)

	hostRole.SetSessionRequirePolicies([]*types.SessionRequirePolicy{{
		Filter: "contains(participant.roles, \"user:host\")",
		Kinds:  []string{string(types.SSHSessionKind)},
		Count:  2,
	}})

	return startTestCase{
		host:        hostRole,
		sessionKind: types.SSHSessionKind,
		participants: []SessionAccessContext{
			{
				Username: "participant",
				Roles:    []types.Role{participantRole},
			},
			{
				Username: "participant2",
				Roles:    []types.Role{participantRole},
			},
		},
		expected: false,
	}
}

func TestSessionAccessStart(t *testing.T) {
	testCases := []startTestCase{
		successStartTestCase(t),
		failCountStartTestCase(t),
		failFilterStartTestCase(t),
	}

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

func successJoinTestCase(t *testing.T) joinTestCase {
	srv := newTestTLSServer(t)
	_, hostRole, err := CreateUserAndRole(srv.Auth(), "host", nil)
	require.NoError(t, err)
	_, participantRole, err := CreateUserAndRole(srv.Auth(), "participant", nil)
	require.NoError(t, err)

	participantRole.SetSessionJoinPolicies([]*types.SessionJoinPolicy{{
		Roles: []string{hostRole.GetName()},
		Kinds: []string{string(types.SSHSessionKind)},
		Modes: []string{string("*")},
	}})

	return joinTestCase{
		host:        hostRole,
		sessionKind: types.SSHSessionKind,
		participant: SessionAccessContext{
			Username: "participant",
			Roles:    []types.Role{participantRole},
		},
		expected: true,
	}
}

func failRoleJoinTestCase(t *testing.T) joinTestCase {
	srv := newTestTLSServer(t)
	_, hostRole, err := CreateUserAndRole(srv.Auth(), "host", nil)
	require.NoError(t, err)
	_, participantRole, err := CreateUserAndRole(srv.Auth(), "participant", nil)
	require.NoError(t, err)

	return joinTestCase{
		host:        hostRole,
		sessionKind: types.SSHSessionKind,
		participant: SessionAccessContext{
			Username: "participant",
			Roles:    []types.Role{participantRole},
		},
		expected: false,
	}
}

func failKindJoinTestCase(t *testing.T) joinTestCase {
	srv := newTestTLSServer(t)
	_, hostRole, err := CreateUserAndRole(srv.Auth(), "host", nil)
	require.NoError(t, err)
	_, participantRole, err := CreateUserAndRole(srv.Auth(), "participant", nil)
	require.NoError(t, err)

	participantRole.SetSessionJoinPolicies([]*types.SessionJoinPolicy{{
		Roles: []string{hostRole.GetName()},
		Kinds: []string{string(types.KubernetesSessionKind)},
		Modes: []string{string("*")},
	}})

	return joinTestCase{
		host:        hostRole,
		sessionKind: types.SSHSessionKind,
		participant: SessionAccessContext{
			Username: "participant",
			Roles:    []types.Role{participantRole},
		},
		expected: false,
	}
}

func TestSessionAccessJoin(t *testing.T) {
	testCases := []joinTestCase{
		successJoinTestCase(t),
		failRoleJoinTestCase(t),
		failKindJoinTestCase(t),
	}

	for _, testCase := range testCases {
		evaluator := NewSessionAccessEvaluator([]types.Role{testCase.host}, testCase.sessionKind)
		result, err := evaluator.CanJoin(testCase.participant)
		require.NoError(t, err)
		require.Equal(t, testCase.expected, len(result) > 0)
	}
}
