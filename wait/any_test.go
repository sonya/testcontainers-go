package wait

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// blockUntilDone is a wait strategy body which never succeeds on its own.
func blockUntilDone(ctx context.Context, _ StrategyTarget) error {
	<-ctx.Done()
	return ctx.Err()
}

// succeedImmediately is a wait strategy body which is ready straight away.
func succeedImmediately(_ context.Context, _ StrategyTarget) error {
	return nil
}

func TestAnyMultiStrategy_WaitsForAny(t *testing.T) {
	t.Parallel()

	// Mirrors the motivating use case: two applications in the same container
	// log different lines, and the caller is happy with either of them.
	appOne := ForNop(blockUntilDone)
	appTwo := ForNop(func(_ context.Context, _ StrategyTarget) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	})

	res := make(chan error, 1)
	go func() {
		res <- ForAny(appOne, appTwo).WaitUntilReady(t.Context(), NopStrategyTarget{})
	}()

	select {
	case err := <-res:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("ForAny did not return once one of its strategies succeeded")
	}
}

func TestAnyMultiStrategy_WaitUntilReady(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		strategy Strategy
		wantErr  bool
	}{
		{
			name:     "returns error when no WaitStrategies are passed",
			strategy: ForAny(),
			wantErr:  true,
		},
		{
			name:     "succeeds when the only strategy succeeds",
			strategy: ForAny(ForNop(succeedImmediately)),
		},
		{
			name: "succeeds when one strategy fails and another succeeds",
			strategy: ForAny(
				ForNop(func(_ context.Context, _ StrategyTarget) error {
					return errors.New("intentional failure")
				}),
				ForNop(succeedImmediately),
			),
		},
		{
			name: "returns error when every strategy fails",
			strategy: ForAny(
				ForNop(func(_ context.Context, _ StrategyTarget) error {
					return errors.New("intentional failure")
				}),
				ForNop(func(_ context.Context, _ StrategyTarget) error {
					return errors.New("intentional failure")
				}),
			),
			wantErr: true,
		},
		{
			name: "WithDeadline sets context Deadline for WaitStrategy",
			strategy: ForAny(
				ForNop(func(ctx context.Context, _ StrategyTarget) error {
					if _, set := ctx.Deadline(); !set {
						return errors.New("expected context.Deadline to be set")
					}
					return nil
				}),
			).WithDeadline(time.Second),
		},
		{
			name: "WithStartupTimeoutDefault sets context Deadline for WaitStrategy",
			strategy: ForAny(
				ForNop(func(ctx context.Context, _ StrategyTarget) error {
					if _, set := ctx.Deadline(); !set {
						return errors.New("expected context.Deadline to be set")
					}
					return nil
				}),
			).WithStartupTimeoutDefault(time.Second),
		},
		{
			name:     "returns error when the deadline elapses first",
			strategy: ForAny(ForNop(blockUntilDone)).WithDeadline(50 * time.Millisecond),
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.strategy.WaitUntilReady(t.Context(), NopStrategyTarget{})
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAnyMultiStrategy_handleNils(t *testing.T) {
	t.Parallel()

	t.Run("only-nils", func(t *testing.T) {
		t.Parallel()

		// Nil strategies are skipped, so there is nothing left to wait for.
		err := ForAny(nil, nil).WaitUntilReady(t.Context(), NopStrategyTarget{})
		require.NoError(t, err)
	})

	t.Run("leading-nil", func(t *testing.T) {
		t.Parallel()

		err := ForAny(nil, ForNop(succeedImmediately)).WaitUntilReady(t.Context(), NopStrategyTarget{})
		require.NoError(t, err)
	})

	t.Run("trailing-typed-nil", func(t *testing.T) {
		t.Parallel()

		var nilStrategy *NopStrategy
		err := ForAny(ForNop(succeedImmediately), nilStrategy).WaitUntilReady(t.Context(), NopStrategyTarget{})
		require.NoError(t, err)
	})
}

func TestAnyMultiStrategy_String(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		require.Equal(t, "any of: (none)", ForAny().String())
	})

	t.Run("nils-are-filtered", func(t *testing.T) {
		var nilStrategy *NopStrategy
		require.Equal(t, "any of: [custom wait condition]", ForAny(ForNop(nil), nilStrategy).String())
	})

	t.Run("multiple", func(t *testing.T) {
		require.Equal(t,
			`any of: [log message "app-1 ready", log message "app-2 ready"]`,
			ForAny(ForLog("app-1 ready"), ForLog("app-2 ready")).String(),
		)
	})
}

func TestAnyMultiStrategy_Observers(t *testing.T) {
	t.Parallel()

	var mtx sync.Mutex
	var kinds []string

	strategy := ForAny(ForNop(succeedImmediately)).WithObserverFunc(func(event Event) {
		mtx.Lock()
		defer mtx.Unlock()
		kinds = append(kinds, event.Kind.String())
	})

	require.NoError(t, strategy.WaitUntilReady(t.Context(), NopStrategyTarget{}))

	mtx.Lock()
	defer mtx.Unlock()
	require.Equal(t, []string{"started", "succeeded"}, kinds)
}

func TestAnyMultiStrategy_Policy(t *testing.T) {
	t.Parallel()

	t.Run("defaults-to-any", func(t *testing.T) {
		require.Equal(t, "quorum(1)", ForAny().Policy().Name())
	})

	t.Run("named-policy", func(t *testing.T) {
		require.Equal(t, "first-result", ForAny().WithPolicyNamed("first-result").Policy().Name())
	})

	t.Run("unknown-named-policy-falls-back", func(t *testing.T) {
		require.Equal(t, "quorum(1)", ForAny().WithPolicyNamed("nope").Policy().Name())
	})
}

func TestQuorumPolicy_Decide(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		required int
		tally    Tally
		want     Decision
	}{
		{name: "any-waits", required: 1, tally: Tally{Total: 2}, want: DecisionWait},
		{name: "any-succeeds", required: 1, tally: Tally{Total: 2, Succeeded: 1}, want: DecisionSucceed},
		{name: "any-fails", required: 1, tally: Tally{Total: 2, Failed: 2}, want: DecisionFail},
		{name: "two-of-three-waits", required: 2, tally: Tally{Total: 3, Succeeded: 1}, want: DecisionWait},
		{name: "two-of-three-succeeds", required: 2, tally: Tally{Total: 3, Succeeded: 2}, want: DecisionSucceed},
		{name: "all-succeeds", required: 0, tally: Tally{Total: 2, Succeeded: 2}, want: DecisionSucceed},
		{name: "all-fails", required: 0, tally: Tally{Total: 2, Succeeded: 1, Failed: 1}, want: DecisionFail},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, QuorumPolicy{Required: tt.required}.Decide(tt.tally))
		})
	}
}

func TestForQuorum(t *testing.T) {
	t.Parallel()

	strategy := ForQuorum(2,
		ForNop(succeedImmediately),
		ForNop(succeedImmediately),
		ForNop(blockUntilDone),
	)

	require.NoError(t, strategy.WaitUntilReady(t.Context(), NopStrategyTarget{}))
}

func TestCompletionPolicyRegistry(t *testing.T) {
	t.Parallel()

	t.Run("built-ins-are-registered", func(t *testing.T) {
		require.Subset(t, RegisteredCompletionPolicies(), []string{"first-result", "quorum(1)", "quorum(all)"})
	})

	t.Run("lookup", func(t *testing.T) {
		policy, err := CompletionPolicyByName("quorum(1)")
		require.NoError(t, err)
		require.Equal(t, DecisionSucceed, policy.Decide(Tally{Total: 3, Succeeded: 1}))
	})

	t.Run("unknown", func(t *testing.T) {
		_, err := CompletionPolicyByName("quorum(7)")
		require.Error(t, err)
	})

	t.Run("duplicate", func(t *testing.T) {
		require.Error(t, RegisterCompletionPolicy(QuorumPolicy{Required: 1}))
	})

	t.Run("nil", func(t *testing.T) {
		require.Error(t, RegisterCompletionPolicy(nil))
	})
}
