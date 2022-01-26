// Copyright 2022 Gravitational, Inc
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

package breaker

import (
	"errors"
	"sync"
	"time"

	"github.com/gravitational/trace"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc/grpclog"
)

type Metrics struct {
	Executions           uint32
	Successes            uint32
	Failures             uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

func (m *Metrics) reset() {
	m.Executions = 0
	m.Successes = 0
	m.Failures = 0
	m.ConsecutiveSuccesses = 0
	m.ConsecutiveFailures = 0
}

func (m *Metrics) success() {
	m.Successes++
	m.ConsecutiveSuccesses++
	m.ConsecutiveFailures = 0
}

func (m *Metrics) failure() {
	m.Failures++
	m.ConsecutiveFailures++
	m.ConsecutiveSuccesses = 0
}

func (m *Metrics) execute() {
	m.Executions++
}

// State represents a state of CircuitBreaker.
type State int

const (
	// StateStandby indicates the breaker is passing all requests and watching stats
	StateStandby State = iota
	// StateTripped indicates too many errors have occurred and requests are actively being rejected
	StateTripped
	// StateRecovering indicates the breaker is allowing some requests to go through, rejecting others
	StateRecovering
)

func (s State) String() string {
	switch s {
	case StateStandby:
		return "standby"
	case StateTripped:
		return "tripped"
	case StateRecovering:
		return "recovering"
	default:
		return "undefined"
	}
}

const (
	defaultTrippedPeriod    = 60 * time.Second
	defaultRecoveryDuration = 10 * time.Second
)

var (
	ErrStateTripped  = errors.New("breaker is tripped")
	ErrLimitExceeded = errors.New("too many requests while recovering")
)

type Config struct {
	Clock          clockwork.Clock
	Interval       time.Duration
	TrippedPeriod  time.Duration
	RecoveryPeriod time.Duration
	RecoveryLimit  uint32
	Trip           TripFn
	OnTripped      func()
	OnStandBy      func()
	IsSuccessful   func(err error) bool
}

type TripFn = func(m Metrics) bool

func RatioTripper(ratio float32, minimum uint32) TripFn {
	return func(m Metrics) bool {
		if m.Executions <= minimum {
			return false
		}
		return float32(m.ConsecutiveFailures)/float32(m.Executions) > ratio
	}
}

func StaticTripper(b bool) TripFn {
	return func(m Metrics) bool {
		return b
	}
}

func ConsecutiveFailureTripper(max uint32) TripFn {
	return func(m Metrics) bool {
		return m.ConsecutiveFailures > max
	}
}

func defaultIsSuccess(err error) bool {
	return err == nil
}

func (c *Config) CheckAndSetDefaults() error {
	if c.Clock == nil {
		c.Clock = clockwork.NewRealClock()
	}

	if c.Interval <= 0 {
		return trace.BadParameter("CircuitBreaker Interval must be set")
	}

	if c.Trip == nil {
		return trace.BadParameter("CircuitBreaker Trip must be set")
	}

	if c.TrippedPeriod <= 0 {
		c.TrippedPeriod = defaultTrippedPeriod
	}

	if c.RecoveryPeriod <= 0 {
		c.RecoveryPeriod = defaultRecoveryDuration
	}

	if c.RecoveryLimit <= 0 {
		c.RecoveryLimit = 1
	}

	if c.OnTripped == nil {
		c.OnTripped = func() {}
	}

	if c.OnStandBy == nil {
		c.OnStandBy = func() {}
	}

	if c.IsSuccessful == nil {
		c.IsSuccessful = defaultIsSuccess
	}

	return nil
}

type CircuitBreaker struct {
	cfg Config

	rc *ratioController

	mu         sync.Mutex
	state      State
	generation uint64
	metrics    Metrics
	expiry     time.Time
}

func New(cfg Config) (*CircuitBreaker, error) {
	if err := cfg.CheckAndSetDefaults(); err != nil {
		return nil, err
	}

	cb := CircuitBreaker{
		cfg: cfg,
	}

	cb.nextGeneration(cfg.Clock.Now())

	return &cb, nil
}

func (c *CircuitBreaker) Execute(f func() error) error {
	generation, err := c.beforeExecution()
	if err != nil {
		return err
	}

	err = f()

	c.afterExecution(generation, err)

	return err
}

func (c *CircuitBreaker) beforeExecution() (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.cfg.Clock.Now()

	generation, state := c.currentSate(now)

	switch {
	case state == StateTripped:
		return generation, ErrStateTripped
	case state == StateRecovering && !c.rc.allowRequest():
		return generation, ErrLimitExceeded
	}

	c.metrics.execute()
	return generation, nil
}

func (c *CircuitBreaker) afterExecution(prior uint64, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := c.cfg.Clock.Now()

	generation, state := c.currentSate(now)
	if generation != prior {
		return
	}

	if c.cfg.IsSuccessful(err) {
		c.success(state, now)
		grpclog.Infof("[breaker] successful execution")
	} else {
		c.failure(state, now)

		grpclog.Infof("[breaker] failed execution consecutive=%d, total=%d execution=%d", c.metrics.ConsecutiveFailures, c.metrics.Failures, c.metrics.Executions)
	}
}

func (c *CircuitBreaker) success(state State, t time.Time) {
	switch state {
	case StateStandby:
		c.metrics.success()
	case StateRecovering:
		c.metrics.success()
		if c.metrics.ConsecutiveSuccesses >= c.cfg.RecoveryLimit {
			c.setState(StateStandby, t)
			go c.cfg.OnStandBy()
		}
	}
}

func (c *CircuitBreaker) failure(state State, t time.Time) {
	switch state {
	case StateRecovering:
		c.setState(StateTripped, t)
	case StateStandby:
		c.metrics.failure()

		if c.cfg.Trip(c.metrics) {
			c.setState(StateTripped, t)
			go c.cfg.OnTripped()
		}
	}
}

func (c *CircuitBreaker) setState(s State, t time.Time) {
	if c.state == s {
		return
	}

	grpclog.Infof("[breaker] state is now %s", s)

	if s == StateRecovering {
		c.rc = newRatioController(c.cfg.Clock, c.cfg.RecoveryPeriod)
	}

	c.state = s
	c.nextGeneration(t)
}

func (c *CircuitBreaker) currentSate(t time.Time) (uint64, State) {
	switch {
	case c.state == StateTripped && c.expiry.Before(t):
		c.setState(StateRecovering, t)
	case c.state == StateStandby && !c.expiry.IsZero() && c.expiry.Before(t):
		c.nextGeneration(t)
	}

	return c.generation, c.state
}

func (c *CircuitBreaker) nextGeneration(t time.Time) {
	c.metrics.reset()
	c.generation++

	switch c.state {
	case StateRecovering:
		c.expiry = time.Time{}
	case StateTripped:
		c.expiry = t.Add(c.cfg.TrippedPeriod)
	case StateStandby:
		c.expiry = t.Add(c.cfg.Interval)
	}
}
