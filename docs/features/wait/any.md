# Any Wait strategy

The Any wait strategy holds a list of wait strategies which are evaluated
concurrently. It completes as soon as one of them succeeds, and the remaining
strategies are cancelled.

This is useful when a container runs more than one application and any one of
them being up is enough to start the test, for example when two services log
different ready lines.

Available Options:

- `WithDeadline` - the deadline for when one strategy must have completed by, default is none.
- `WithStartupTimeoutDefault` - the startup timeout default to be used for each Strategy if not defined, default is 60 seconds.
- `WithPolicy` - the `CompletionPolicy` used to decide when the combinator is done, default is `QuorumPolicy{Required: 1}`.
- `WithPolicyNamed` - as `WithPolicy`, resolving the policy from the registry by name.
- `WithFailureTolerance` - the number of inner strategies allowed to fail before giving up, default is no cap.
- `WithConcurrencyLimit` - the number of inner strategies allowed to run at once, default is unlimited.
- `WithObserver` / `WithObserverFunc` - receive an `Event` for each inner strategy transition.

```golang
req := ContainerRequest{
    Image:        "my/multi-app:latest",
    ExposedPorts: []string{"8080/tcp"},
    WaitingFor: wait.ForAny(
        wait.ForLog("app-1 listening on 8080"),
        wait.ForLog("app-2 listening on 8080"),
    ).WithDeadline(120 * time.Second),
}
```

## Quorum

`ForQuorum` is the general form of `ForAny`, and waits until the requested
number of strategies have succeeded:

```golang
wait.ForQuorum(2,
    wait.ForLog("shard-a ready"),
    wait.ForLog("shard-b ready"),
    wait.ForLog("shard-c ready"),
)
```

## Custom policies

A `CompletionPolicy` receives a `Tally` of the results seen so far and returns
a `Decision`. Policies can be registered by name so that a combinator can be
assembled from configuration:

```golang
_ = wait.RegisterCompletionPolicy(wait.PolicyFunc{
    PolicyName: "majority",
    Fn: func(tally wait.Tally) wait.Decision {
        switch {
        case tally.Succeeded > tally.Total/2:
            return wait.DecisionSucceed
        case tally.Pending() == 0:
            return wait.DecisionFail
        default:
            return wait.DecisionWait
        }
    },
})

wait.ForAny(strategies...).WithPolicyNamed("majority")
```
