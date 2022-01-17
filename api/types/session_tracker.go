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

package types

import (
	"time"

	"github.com/gravitational/trace"
)

const (
	SSHSessionKind        SessionKind            = "ssh"
	KubernetesSessionKind SessionKind            = "k8s"
	SessionObserverMode   SessionParticipantMode = "observer"
	SessionModeratorMode  SessionParticipantMode = "moderator"
	SessionPeerMode       SessionParticipantMode = "peer"
)

type SessionKind string
type SessionParticipantMode string

type SessionTracker interface {
	Resource

	GetID() string

	GetNamespace() string

	GetSessionKind() SessionKind

	GetState() SessionState

	SetState(SessionState) error

	GetCreated() time.Time

	GetExpires() time.Time

	GetReason() string

	GetInvited() []string

	GetHostname() string

	GetAddress() string

	GetClustername() string

	GetLogin() string

	GetParticipants() []*Participant

	AddParticipant(*Participant)

	RemoveParticipant(string) error

	UpdatePresence(string) error

	GetKubeCluster() string

	GetHostUser() string
}

func NewSession(spec SessionTrackerSpecV1) (SessionTracker, error) {
	meta := Metadata{
		Name: spec.SessionID,
	}

	session := &SessionTrackerV1{
		Kind:     KindSessionTracker,
		Version:  V1,
		Metadata: meta,
		Spec:     spec,
	}

	if err := session.Metadata.CheckAndSetDefaults(); err != nil {
		return nil, trace.Wrap(err)
	}

	return session, nil
}

// GetVersion returns resource version.
func (c *SessionTrackerV1) GetVersion() string {
	return c.Version
}

// GetName returns the name of the resource.
func (c *SessionTrackerV1) GetName() string {
	return c.Metadata.Name
}

// SetName sets the name of the resource.
func (c *SessionTrackerV1) SetName(e string) {
	c.Metadata.Name = e
}

// SetExpiry sets expiry time for the object.
func (c *SessionTrackerV1) SetExpiry(expires time.Time) {
	c.Metadata.SetExpiry(expires)
}

// Expiry returns object expiry setting.
func (c *SessionTrackerV1) Expiry() time.Time {
	return c.Metadata.Expiry()
}

// GetMetadata returns object metadata.
func (c *SessionTrackerV1) GetMetadata() Metadata {
	return c.Metadata
}

// GetResourceID returns resource ID.
func (c *SessionTrackerV1) GetResourceID() int64 {
	return c.Metadata.ID
}

// SetResourceID sets resource ID.
func (c *SessionTrackerV1) SetResourceID(id int64) {
	c.Metadata.ID = id
}

// GetKind returns resource kind.
func (c *SessionTrackerV1) GetKind() string {
	return c.Kind
}

// GetSubKind returns resource subkind.
func (c *SessionTrackerV1) GetSubKind() string {
	return c.SubKind
}

// SetSubKind sets resource subkind.
func (c *SessionTrackerV1) SetSubKind(sk string) {
	c.SubKind = sk
}

func (s *SessionTrackerV1) CheckAndSetDefaults() error {
	s.Kind = KindSessionTracker
	s.Version = V3

	if err := s.Metadata.CheckAndSetDefaults(); err != nil {
		return trace.Wrap(err)
	}

	return nil
}

func (s *SessionTrackerV1) GetID() string {
	return s.Spec.SessionID
}

func (s *SessionTrackerV1) GetNamespace() string {
	return s.Spec.Namespace
}

func (s *SessionTrackerV1) GetSessionKind() SessionKind {
	return SessionKind(s.Spec.Type)
}

func (s *SessionTrackerV1) GetState() SessionState {
	return s.Spec.State
}

func (s *SessionTrackerV1) SetState(state SessionState) error {
	switch state {
	default:
		return trace.BadParameter("invalid session state: %v", state)
	case SessionState_SessionStateRunning:
		fallthrough
	case SessionState_SessionStatePending:
		fallthrough
	case SessionState_SessionStateTerminated:
		s.Spec.State = state
		return nil
	}
}

func (s *SessionTrackerV1) GetCreated() time.Time {
	return s.Spec.Created
}

func (s *SessionTrackerV1) GetExpires() time.Time {
	return s.Spec.Expires
}

func (s *SessionTrackerV1) GetReason() string {
	return s.Spec.Reason
}

func (s *SessionTrackerV1) GetInvited() []string {
	return s.Spec.Invited
}

func (s *SessionTrackerV1) GetHostname() string {
	return s.Spec.Hostname
}

func (s *SessionTrackerV1) GetAddress() string {
	return s.Spec.Address
}

func (s *SessionTrackerV1) GetClustername() string {
	return s.Spec.ClusterName
}

func (s *SessionTrackerV1) GetLogin() string {
	return s.Spec.Login
}

func (s *SessionTrackerV1) GetParticipants() []*Participant {
	return s.Spec.Participants
}

func (s *SessionTrackerV1) AddParticipant(participant *Participant) {
	s.Spec.Participants = append(s.Spec.Participants, participant)
}

func (s *SessionTrackerV1) RemoveParticipant(id string) error {
	for i, participant := range s.Spec.Participants {
		if participant.ID == id {
			s.Spec.Participants = append(s.Spec.Participants[:i], s.Spec.Participants[i+1:]...)
			return nil
		}
	}

	return trace.BadParameter("participant %v not found", id)
}

func (s *SessionTrackerV1) GetKubeCluster() string {
	return s.Spec.KubernetesCluster
}

func (s *SessionTrackerV1) GetHostUser() string {
	return s.Spec.HostUser
}

func (s *SessionTrackerV1) UpdatePresence(user string) error {
	for _, participant := range s.Spec.Participants {
		if participant.User == user {
			participant.LastActive = time.Now().UTC()
			return nil
		}
	}

	return trace.BadParameter("participant %v not found", user)
}
