package wait

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

// Implement interface
var (
	_ Strategy        = (*AnyMultiStrategy)(nil)
	_ StrategyTimeout = (*AnyMultiStrategy)(nil)
)

// Decision is the verdict a [CompletionPolicy] returns after each inner
// strategy reports in. It tells the combinator whether it can stop waiting.
type Decision int

const (
	// DecisionWait means the policy needs to see more results.
	DecisionWait Decision = iota

	// DecisionSucceed means the combinator is satisfied.
	DecisionSucceed

	// DecisionFail means the combinator can no longer be satisfied.
	DecisionFail
)

// String returns a human-readable description of the decision.
func (d Decision) String() string {
	switch d {
	case DecisionWait:
		return "wait"
	case DecisionSucceed:
		return "succeed"
	case DecisionFail:
		return "fail"
	default:
		return fmt.Sprintf("Decision(%d)", int(d))
	}
}

// Tally is a point-in-time summary of the inner strategies of a combinator.
// It is passed to a [CompletionPolicy] so the policy can stay stateless.
type Tally struct {
	// Total is the number of strategies that were actually started.
	Total int

	// Succeeded is the number of strategies that have returned a nil error.
	Succeeded int

	// Failed is the number of strategies that have returned a non-nil error.
	Failed int
}

// Pending returns the number of strategies which have not reported yet.
func (t Tally) Pending() int {
	return t.Total - t.Succeeded - t.Failed
}

// CompletionPolicy decides when a combinator of wait strategies is done.
// Implementations must be safe to share between combinators and must not
// retain any state between calls to Decide.
type CompletionPolicy interface {
	// Name uniquely identifies the policy, and is used as the registry key.
	Name() string

	// Decide reports whether the results seen so far are terminal.
	Decide(tally Tally) Decision
}

// PolicyFunc adapts a plain function into a [CompletionPolicy], for callers
// who do not want to declare a named type.
type PolicyFunc struct {
	// PolicyName is returned by Name.
	PolicyName string

	// Fn is invoked by Decide.
	Fn func(tally Tally) Decision
}

// Name implements [CompletionPolicy].
func (p PolicyFunc) Name() string {
	return p.PolicyName
}

// Decide implements [CompletionPolicy].
func (p PolicyFunc) Decide(tally Tally) Decision {
	return p.Fn(tally)
}

// QuorumPolicy is satisfied once Required strategies have succeeded. It is the
// general form of both "any" (Required == 1) and "all" (Required == Total).
type QuorumPolicy struct {
	// Required is the number of successes needed. A value of zero or less is
	// interpreted as "every strategy must succeed".
	Required int
}

// Name implements [CompletionPolicy].
func (q QuorumPolicy) Name() string {
	if q.Required <= 0 {
		return "quorum(all)"
	}
	return fmt.Sprintf("quorum(%d)", q.Required)
}

// Decide implements [CompletionPolicy].
func (q QuorumPolicy) Decide(tally Tally) Decision {
	required := q.Required
	if required <= 0 {
		required = tally.Total
	}

	if tally.Succeeded >= required {
		return DecisionSucceed
	}

	if tally.Pending() == 0 {
		return DecisionFail
	}

	return DecisionWait
}

var (
	policyMu       sync.RWMutex
	policyRegistry = map[string]CompletionPolicy{}
)

// RegisterCompletionPolicy makes a policy available by name so that
// combinators can be built from configuration rather than from code.
// It returns an error if a policy with the same name is already registered.
func RegisterCompletionPolicy(policy CompletionPolicy) error {
	if policy == nil {
		return errors.New("nil completion policy")
	}

	policyMu.Lock()
	defer policyMu.Unlock()

	name := policy.Name()
	if _, ok := policyRegistry[name]; ok {
		return fmt.Errorf("completion policy %q already registered", name)
	}
	policyRegistry[name] = policy

	return nil
}

// CompletionPolicyByName looks up a policy previously passed to
// [RegisterCompletionPolicy].
func CompletionPolicyByName(name string) (CompletionPolicy, error) {
	policyMu.RLock()
	defer policyMu.RUnlock()

	policy, ok := policyRegistry[name]
	if !ok {
		return nil, fmt.Errorf("unknown completion policy %q", name)
	}

	return policy, nil
}

// RegisteredCompletionPolicies returns the sorted names of every registered
// policy, which is useful for surfacing the available options to users.
func RegisteredCompletionPolicies() []string {
	policyMu.RLock()
	defer policyMu.RUnlock()

	names := make([]string, 0, len(policyRegistry))
	for name := range policyRegistry {
		names = append(names, name)
	}
	sort.Strings(names)

	return names
}

func init() {
	// The built-in policies are registered eagerly so that they can be
	// referenced by name from the very first call.
	_ = RegisterCompletionPolicy(QuorumPolicy{Required: 1})
	_ = RegisterCompletionPolicy(QuorumPolicy{Required: 0})
	_ = RegisterCompletionPolicy(PolicyFunc{
		PolicyName: "first-result",
		Fn: func(tally Tally) Decision {
			if tally.Succeeded > 0 {
				return DecisionSucceed
			}
			if tally.Failed > 0 {
				return DecisionFail
			}
			return DecisionWait
		},
	})
}

// EventKind identifies the lifecycle transition an [Event] describes.
type EventKind int

const (
	// EventStrategyStarted is emitted just before an inner strategy runs.
	EventStrategyStarted EventKind = iota

	// EventStrategySucceeded is emitted when an inner strategy returns nil.
	EventStrategySucceeded

	// EventStrategyFailed is emitted when an inner strategy returns an error.
	EventStrategyFailed

	// EventStrategyCancelled is emitted when an inner strategy is abandoned
	// because the combinator has already reached a decision.
	EventStrategyCancelled
)

// String returns a human-readable description of the event kind.
func (k EventKind) String() string {
	switch k {
	case EventStrategyStarted:
		return "started"
	case EventStrategySucceeded:
		return "succeeded"
	case EventStrategyFailed:
		return "failed"
	case EventStrategyCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("EventKind(%d)", int(k))
	}
}

// Event describes something that happened to an inner strategy of a
// combinator. It is delivered to every registered [Observer].
type Event struct {
	// Kind is the transition being reported.
	Kind EventKind

	// Index is the position of the strategy in the combinator's Strategies.
	Index int

	// Strategy is the inner strategy the event relates to.
	Strategy Strategy

	// Err is the error returned by the strategy, if any.
	Err error

	// At is the time the event was produced.
	At time.Time
}

// Observer receives [Event] values from a combinator.
type Observer interface {
	Observe(event Event)
}

// ObserverFunc adapts a plain function into an [Observer].
type ObserverFunc func(event Event)

// Observe implements [Observer].
func (f ObserverFunc) Observe(event Event) {
	f(event)
}

// AnyMultiStrategy holds a list of wait strategies which are evaluated
// concurrently, and completes as soon as its [CompletionPolicy] is satisfied.
type AnyMultiStrategy struct {
	// all Strategies should have a startupTimeout to avoid waiting infinitely
	timeout  *time.Duration
	deadline *time.Duration

	// policy decides when the combinator is done, defaulting to "any".
	policy CompletionPolicy

	// failureTolerance caps how many inner strategies may fail before the
	// combinator gives up. Nil means no cap.
	failureTolerance *int

	// concurrencyLimit caps how many inner strategies run at once. Zero or
	// less means unlimited.
	concurrencyLimit int

	// observers are notified of every inner strategy transition.
	observers []Observer

	// additional properties
	Strategies []Strategy
}

// ForAny returns a wait strategy that waits for any of the supplied conditions
// to become true, after which the remaining ones are cancelled.
//
// Failures are not permitted: any strategy which fails will have its error
// immediately returned.
func ForAny(strategies ...Strategy) *AnyMultiStrategy {
	return &AnyMultiStrategy{
		Strategies: strategies,
	}
}

// ForQuorum returns a wait strategy that waits until required of the supplied
// conditions have become true. It is the general form of [ForAny].
func ForQuorum(required int, strategies ...Strategy) *AnyMultiStrategy {
	return ForAny(strategies...).WithPolicy(QuorumPolicy{Required: required})
}

// WithStartupTimeoutDefault sets the default timeout for all inner wait strategies.
func (ms *AnyMultiStrategy) WithStartupTimeoutDefault(timeout time.Duration) *AnyMultiStrategy {
	ms.timeout = &timeout
	return ms
}

// WithDeadline sets a time.Duration which limits all wait strategies.
func (ms *AnyMultiStrategy) WithDeadline(deadline time.Duration) *AnyMultiStrategy {
	ms.deadline = &deadline
	return ms
}

// WithPolicy overrides the [CompletionPolicy] used to decide when the
// combinator is done.
func (ms *AnyMultiStrategy) WithPolicy(policy CompletionPolicy) *AnyMultiStrategy {
	ms.policy = policy
	return ms
}

// WithPolicyNamed overrides the [CompletionPolicy] using a name previously
// passed to [RegisterCompletionPolicy].
func (ms *AnyMultiStrategy) WithPolicyNamed(name string) *AnyMultiStrategy {
	policy, err := CompletionPolicyByName(name)
	if err != nil {
		// Fall back to the default rather than panicking, so that a typo in
		// configuration cannot take down a test suite.
		return ms
	}

	return ms.WithPolicy(policy)
}

// WithFailureTolerance caps the number of inner strategies which may fail
// before the combinator gives up.
func (ms *AnyMultiStrategy) WithFailureTolerance(tolerance int) *AnyMultiStrategy {
	ms.failureTolerance = &tolerance
	return ms
}

// WithConcurrencyLimit caps the number of inner strategies which run at the
// same time, which keeps resource usage predictable when a container is
// waited on by a large number of conditions.
func (ms *AnyMultiStrategy) WithConcurrencyLimit(limit int) *AnyMultiStrategy {
	ms.concurrencyLimit = limit
	return ms
}

// WithObserver registers an [Observer] which is notified of every inner
// strategy transition.
func (ms *AnyMultiStrategy) WithObserver(observer Observer) *AnyMultiStrategy {
	ms.observers = append(ms.observers, observer)
	return ms
}

// WithObserverFunc registers a function which is notified of every inner
// strategy transition.
func (ms *AnyMultiStrategy) WithObserverFunc(fn func(event Event)) *AnyMultiStrategy {
	return ms.WithObserver(ObserverFunc(fn))
}

// Timeout implements [StrategyTimeout].
func (ms *AnyMultiStrategy) Timeout() *time.Duration {
	return ms.deadline
}

// Policy returns the effective [CompletionPolicy] of the combinator.
func (ms *AnyMultiStrategy) Policy() CompletionPolicy {
	if ms.policy != nil {
		return ms.policy
	}

	return QuorumPolicy{Required: 1}
}

// String returns a human-readable description of the wait strategy.
func (ms *AnyMultiStrategy) String() string {
	if len(ms.Strategies) == 0 {
		return "any of: (none)"
	}

	var strategies []string
	for _, strategy := range ms.Strategies {
		if strategy == nil || reflect.ValueOf(strategy).IsNil() {
			continue
		}
		if s, ok := strategy.(fmt.Stringer); ok {
			strategies = append(strategies, s.String())
		} else {
			strategies = append(strategies, fmt.Sprintf("%T", strategy))
		}
	}

	// Always include "any of:" prefix to make it clear this is an
	// AnyMultiStrategy even when there's only one strategy after filtering
	// out nils.
	return "any of: [" + strings.Join(strategies, ", ") + "]"
}

// notify delivers an event to every registered observer.
func (ms *AnyMultiStrategy) notify(kind EventKind, index int, strategy Strategy, err error) {
	if len(ms.observers) == 0 {
		return
	}

	event := Event{
		Kind:     kind,
		Index:    index,
		Strategy: strategy,
		Err:      err,
		At:       time.Now(),
	}
	for _, observer := range ms.observers {
		observer.Observe(event)
	}
}

// result carries the outcome of a single inner strategy.
type result struct {
	index    int
	strategy Strategy
	err      error
}

func (ms *AnyMultiStrategy) WaitUntilReady(ctx context.Context, target StrategyTarget) error {
	if len(ms.Strategies) == 0 {
		return errors.New("no wait strategy supplied")
	}

	// Retained so that per-strategy timeouts can be derived without inheriting
	// a cancellation that has already fired.
	parent := ctx

	ctx, cancel := context.WithCancel(ctx)
	defer cancel() // All remaining strategies will stop when this fires.

	if ms.deadline != nil {
		ctx, cancel = context.WithTimeout(ctx, *ms.deadline)
		defer cancel()
	}

	// Filter out the nil strategies up front so that the tally, the policy and
	// the observers all agree on how many strategies are in play.
	live := make([]result, 0, len(ms.Strategies))
	for i, strategy := range ms.Strategies {
		if strategy == nil || reflect.ValueOf(strategy).IsNil() {
			// A module could be appending strategies after part of the container
			// initialization, and use wait.ForAny on a not initialized strategy.
			// In this case, we just skip the nil strategy.
			continue
		}
		live = append(live, result{index: i, strategy: strategy})
	}

	if len(live) == 0 {
		return nil
	}

	var sem chan struct{}
	if ms.concurrencyLimit > 0 {
		sem = make(chan struct{}, ms.concurrencyLimit)
	}

	resCh := make(chan result, len(live))
	for _, item := range live {
		strategyCtx := ctx

		// Set default Timeout when strategy implements StrategyTimeout
		if st, ok := item.strategy.(StrategyTimeout); ok {
			if ms.timeout != nil && st.Timeout() == nil {
				var timeoutCancel context.CancelFunc
				strategyCtx, timeoutCancel = context.WithTimeout(parent, *ms.timeout)
				defer timeoutCancel()
			}
		}

		go func(item result, strategyCtx context.Context) {
			if sem != nil {
				sem <- struct{}{}
				defer func() { <-sem }()
			}

			ms.notify(EventStrategyStarted, item.index, item.strategy, nil)
			item.err = item.strategy.WaitUntilReady(strategyCtx, target)
			resCh <- item
		}(item, strategyCtx)
	}

	policy := ms.Policy()
	tally := Tally{Total: len(live)}
	var lastErr error

	for {
		select {
		case res := <-resCh:
			if res.err != nil {
				tally.Failed++
				lastErr = res.err
				ms.notify(EventStrategyFailed, res.index, res.strategy, res.err)
			} else {
				tally.Succeeded++
				ms.notify(EventStrategySucceeded, res.index, res.strategy, nil)
			}

			if ms.failureTolerance != nil && tally.Failed > *ms.failureTolerance {
				return fmt.Errorf("failure tolerance of %d exceeded: %w", *ms.failureTolerance, lastErr)
			}

			switch policy.Decide(tally) {
			case DecisionSucceed:
				return nil
			case DecisionFail:
				return fmt.Errorf("%s not satisfied: %w", policy.Name(), lastErr)
			case DecisionWait:
			}
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for strategies: %w", ctx.Err())
		}
	}
}
