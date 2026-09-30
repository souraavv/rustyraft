<p align="center">
  <img src="./misc/image.png" alt="Rusty Raft" width="400">
</p>

- [RustyRaft](#rustyraft)
- [RustyRaft design and testing notes](#rustyraft-design-and-testing-notes)
- [References](#references)

### RustyRaft

Rust implementation of the Raft consensus algorithm.

The goal of this project is not just to implement Raft, but to understand
how a distributed consensus system works by building it piece by piece.

The implementation is being designed so that the same Raft logic can later
be tested with deterministic failures such as:

- dropped messages
- delayed messages
- reordered messages
- duplicated messages
- network partitions
- node crashes
- node restarts
- storage failures

The main design principle is simple:
Keep the Raft protocol separate from networking, storage and timing.

This makes the core logic easier to understand and, easier to test.

### RustyRaft design and testing notes

What was decided, why it was decided, and what the tests are trying to prove.

This is not a replacement for the Raft paper. It is a working note for this implementation. The goal is to keep the important design decisions visible so that the same way of thinking can be used in another system later.

**The main approach**

Build one small rule at a time and connect the pieces only after the rule is understood and tested.

The current path has been:

- basic types and state
- log
- RequestVote and election
- AppendEntries
- replication progress
- commit index
- state machine
- transport
- message failures
- restart
- cluster level testing

The main testing rule is simple:

- a small test should tell which rule is broken
- a failing test should teach something about the design
- avoid a large test that only says that "Raft is broken"

**Keep Raft logic separate from the environment**

Raft should not know how the outside world works.

Keep these concerns separate:

- Raft protocol logic
- transport
- timers
- persistent storage
- state machine
- test cluster

That gives a useful property: the same Raft logic can run with an in-memory transport during tests and later with a real network. The protocol cares about the message and the state transition, not whether the message came through TCP, a test queue, or something else.

This separation is also what makes failure testing practical. A test transport can drop or reorder a message without adding fake failure logic to the Raft node itself.

**Keep protocol rules small**

A function should answer one Raft question when possible.

Examples:

- Is this candidate log up to date?
- Does this AppendEntries prefix match?
- Did this follower replicate the entry?
- Can the leader advance commit_index?
- Has the election timer expired?

Avoid one large function that checks the log, changes the role, sends messages, updates timers, and changes commit state all at once. Smaller rules are easier to explain and easier to test.

**Use strong types for protocol values**

Do not use plain `u64` for everything.

Use separate types for:

- `Term`
- `LogIndex`
- `ServerId`

They all hold numbers, but they mean different things. Separate types make the code easier to read and make some wrong combinations harder to write by mistake.

**Log index starts at one**

`LogIndex::ZERO` represents the position before the first real log entry.

So a new log looks like:

```text
index 0     -> special empty position
index 1     -> first real entry
index 2     -> second real entry
```

This makes an empty AppendEntries request natural:

```text
prev_log_index = 0
prev_log_term  = 0
```

There is no need to create a fake log entry just to represent the beginning of the log.

**Term is logical time**

A term is not wall-clock time. It is protocol time.

Its main jobs are:

- identify newer and older protocol state
- reject stale RPCs
- force stale leaders or candidates to step down
- make elections and log entries comparable across changes of leadership

The important mental model is:

```text
newer term -> older protocol state is stale
```

Term changes are monotonic.

**Persistent state and volatile state are different**

Persistent state:

- `current_term`
- `voted_for`
- `log`

Volatile state:

- `commit_index`
- `last_applied`

Leader-only volatile state:

- `next_index`
- `match_index`

Keeping these separate makes restart behavior much easier to reason about. After a crash, the persistent part can be used to build a new node while the volatile state can be rebuilt or starts from its initial value.

That is why the implementation has an explicit restart path:

```text
into_persistent_state(...)
        -> old node is gone
        -> persistent state is kept
        -> from_persistent_state(...)
        -> new node continues
```

**Why does `into_persistent_state()` consume the node?**

Because the operation represents the old node going away. There is no reason to clone the persistent state just to create another owner of it.

The ownership model is clearer:

```text
old node
    |
    +--> persistent state
             |
             v
        new node
```

**The log is not the same as commitment**

There are three different ideas:

- the entry exists in the log
- the entry is committed
- the entry has been applied to the state machine

The important state is:

```text
log           -> what has been stored
commit_index  -> what is known to be committed
last_applied  -> what has been executed
```

An entry being present in the log does not mean it is committed. A committed entry also does not mean the state machine has already applied it.

**Leader append-only rule**

The leader appends new entries to its own log. It does not rewrite its own log to fix conflicts.

Conflict repair happens on followers.

The follower can remove its conflicting suffix and append the leader's entries once the AppendEntries consistency check proves that the prefix matches.

This is covered by tests such as:

- `follower_replaces_conflicting_log_entries`
- `leader_and_follower_complete_log_replication_round`

**AppendEntries does two jobs**

AppendEntries is used for both:

- log replication
- heartbeat

An empty `entries` list means heartbeat.

This keeps the protocol smaller because there is no need for a separate heartbeat RPC. The same message also carries `leader_commit`, so heartbeats can tell followers about commit progress.

**Why reject stale AppendEntries before resetting the timer?**

Because an old leader must not be able to keep a follower alive forever by sending an old heartbeat.

The order is important:

```text
receive AppendEntries
        |
        v
check term
        |
   older term? ---- yes ---> reject
        |
        no
        v
process request
        |
        v
reset election timer
```

This was caught by `stale_append_entries_does_not_reset_election_timer`.

That test led to a real design rule: validation must happen before side effects that represent accepted protocol activity.

**Current-term AppendEntries makes a candidate step down**

A candidate that receives a valid AppendEntries from the current term has evidence that another server is acting as leader.

So it stops being a candidate and returns to follower state.

Test:

- `candidate_steps_down_on_current_term_append_entries`

The same general rule applies when a server sees a newer term: stale role state must not survive a newer protocol state.

**Election timers are deterministic in tests**

Tests use ticks instead of real sleep calls.

That gives predictable checks such as:

```text
4 ticks -> still follower
5th tick -> election starts
```

Important tests include:

- `follower_does_not_start_election_before_timeout`
- `follower_starts_election_when_election_timer_expires`
- `candidate_starts_another_election_after_timeout`
- `starting_another_election_increments_term_again`
- `candidate_does_not_start_another_election_before_timeout`
- `append_entries_resets_follower_election_timer`
- `granting_vote_resets_election_timer`
- `stale_append_entries_does_not_reset_election_timer`

Deterministic time makes protocol tests much easier to reproduce than tests based on actual elapsed milliseconds.

**Granting a vote resets the election timer**

A valid vote is also election activity.

After granting a vote, the follower resets its election timer. Otherwise it could grant a vote and then immediately time out and start another election.

Test:

- `granting_vote_resets_election_timer`

This is a good example of a test exposing a missing protocol side effect.

**RequestVote tests**

The RequestVote tests cover:

- older term is rejected
- newer term updates `current_term`
- granted vote is recorded in `voted_for`
- a different candidate cannot get a second vote in the same term
- the same candidate can be handled again correctly
- candidate log freshness is checked
- granting a vote resets election timing

The useful design lesson is that `voted_for` is not just a field to store. It exists because the election safety rule needs memory of what happened earlier in the term.

**Election tests use real transitions**

The tests normally do this:

```text
start_election()
    -> receive vote response
    -> become leader
```

They do not simply construct a leader state by hand.

This checks the real transition and catches mistakes in the election path itself.

Covered behavior includes:

- candidate becomes leader after a majority
- candidate stays candidate without enough votes
- candidate starts another election after timeout
- every new election increases the term
- current-term AppendEntries makes a candidate step down

**Initial heartbeat from a new leader**

A newly elected leader should establish its authority immediately instead of waiting for the normal heartbeat interval.

The design is:

```text
leader elected
    |
    v
initial heartbeat immediately
    |
    v
normal heartbeat timer
    |
    v
next heartbeat after interval
```

This is represented by `initial_heartbeat_pending`.

One old test expected no heartbeat before the first interval. Once the design changed, that test became stale and was changed to check for the second heartbeat instead.

The lesson is important: tests describe the current contract. When the contract changes for a good reason, update the test rather than weakening the new behavior just to keep an old test green.

**Heartbeat tests**

The heartbeat tests cover:

- leader produces a heartbeat for each follower
- a heartbeat is empty
- the first heartbeat is immediate after election
- another heartbeat does not appear before the normal interval
- a valid heartbeat resets the follower election timer
- a stale heartbeat does not reset the timer
- a successful heartbeat does not advance `match_index`

That last point matters because:

```text
RPC success != log replication progress
```

An empty heartbeat proves contact, but it does not prove that a new log entry was replicated.

**Why are `next_index` and `match_index` separate?**

`next_index` answers:

> Which log entry should be sent next?

`match_index` answers:

> What is the highest log entry known to be on this follower?

They serve different jobs:

- `next_index` drives retry and replication
- `match_index` drives commit calculation

Keeping them separate makes the leader logic much easier to reason about.

**Replication progress is monotonic where it should be**

Successful replication moves confirmed progress forward.

Tests cover:

- `follower_progress_starts_at_given_next_index`
- `successful_replication_advances_progress`
- `stale_success_does_not_move_match_index_backwards`
- `failed_replication_moves_next_index_back`
- `failed_replication_does_not_go_below_zero`
- `replication_state_initializes_each_follower`
- `replication_state_updates_correct_follower`
- `unknown_follower_returns_false`
- `unknown_follower_has_no_progress`

The key rule is that late packets should not make confirmed progress go backwards.

**AppendEntries retry**

When a follower rejects AppendEntries because the prefix does not match, the leader moves `next_index` backward and retries.

The flow is:

```text
build AppendEntries
        |
        v
follower rejects
        |
        v
next_index moves back
        |
        v
build again
        |
        v
matching prefix found
        |
        v
entry replicated
```

This is a protocol recovery path, not an exceptional one-off case.

**Why does the response contain `replicated_index`?**

The response belongs to the follower.

It should contain enough information for the leader to understand what happened without looking at the leader's own log and guessing.

The response therefore carries the follower's replicated position.

This fixed a test-harness smell where the leader's own `last_log_index()` was being used to infer what the follower had actually stored.

General rule:

- do not reconstruct remote state from local state when the remote message can report the fact directly

**Why was a `MessageId` not added?**

A `MessageId` was considered and intentionally left out.

For the current protocol behavior, the important state already comes from:

- term checks
- `next_index`
- monotonic `match_index`
- the message payload itself

Adding another identifier would mean another piece of state, another rule, and more tests without solving a current Raft correctness problem.

The rule is not "never use MessageId". The rule is "add it when a real problem needs it".

**Transport is controlled by tests**

The in-memory transport can:

- send messages
- inspect pending messages
- deliver the next message
- deliver to a specific server
- deliver a message at a chosen position
- drop messages to a server
- report pending message count

This makes it possible to test the same Raft code under normal and bad network behavior.

The protocol does not need special branches for "dropped message mode". The transport simply decides whether the message arrives.

**Duplicate, delayed and reordered messages**

Distributed systems should not depend on messages arriving exactly once and in perfect order.

The tests therefore exercise:

- dropped messages
- delayed messages
- reordered messages
- duplicate messages
- late responses

The design response is mostly monotonic state and careful term checking.

Examples:

- stale replication success should not reduce `match_index`
- stale AppendEntries should not reset the election timer
- a duplicate request should not create extra committed progress

**Commit logic is separate from replication**

Replication answers:

> Did the follower store the entry?

Commit answers:

> Is the entry now committed?

Those are connected, but they are not the same decision.

This separation is deliberate because it keeps majority calculation and the current-term rule visible instead of hiding them inside low-level replication code.

**Leader commit rule**

For a three-node cluster:

```text
leader        = 1
follower A    = 1
follower B    = 0
------------------
majority      = 2 / 3
```

So the leader can reach a majority with one follower.

But majority count alone is not enough. The entry being directly committed by the current leader must satisfy the current-term condition used by Raft.

Tests cover:

- leader counts itself
- leader commits after majority replication
- leader does not commit without a majority
- `commit_index` never moves backwards
- an older-term entry is not directly committed by the new leader

**Why does the current-term rule matter?**

A simple majority counter can look correct while still being wrong.

A new leader may have older entries from a previous term. Those entries must not be directly committed just because enough replicas happen to contain them.

That is why `leader_does_not_commit_older_term_entry_directly` exists.

The interesting part is that this is a safety rule that is easy to miss if the implementation is written only around happy-path replication.

**State machine is separate from the log**

The intended flow is:

```text
client command
    |
    v
leader log
    |
    v
replication
    |
    v
commit
    |
    v
state machine
```

The log stores commands.

The state machine executes committed commands.

Appending locally is not enough to run the command.

**State machine application is ordered**

`last_applied` is the boundary between committed work that has been applied and committed work that is still waiting.

Rules:

- apply entries in increasing log index order
- do not skip an earlier failed entry
- only advance `last_applied` after successful application
- do not apply an already-applied entry again

The main failure test is:

- `last_applied_does_not_advance_when_application_fails`

For example:

```text
A -> apply succeeds
B -> apply fails

commit_index = 2
last_applied = 1
```

The failed command can be retried later without pretending it already ran.

**Why use `Rc<RefCell<_>>` in some test helpers?**

Only for the single-threaded test harness.

- `Rc` gives shared ownership
- `RefCell` gives interior mutability with runtime borrow checking

It lets a test observe what the state machine applied without adding production-only inspection methods.

This is a test convenience, not a replacement for proper concurrent synchronization.

**No automatic `Clone` or `Copy`**

`Clone` and `Copy` should have a reason.

Value-like protocol types such as `Term`, `LogIndex`, and `ServerId` are good candidates because they are small values with value semantics.

Commands and log entries are different. Cloning those can have real cost, so ownership and borrowing should be considered first.

Whenever `Clone` or `Copy` is added, the question should be:

- Why is it needed here?
- Can the value be borrowed instead?
- Is the clone hiding an ownership problem?

**Restart and crash behavior**

The restart path keeps durable state explicit.

Tests cover:

- extracting persistent state
- rebuilding a node from persistent state
- preserving `current_term`
- preserving `voted_for`
- preserving the log
- continuing replication after a follower restart

The useful mental model is:

```text
crash
  -> volatile state is gone
  -> persistent state remains
  -> new node is created
  -> replication continues
```

A restart test is more useful when the restarted node actually joins the protocol again, not just when its fields happen to match the old object.

**Test style**

Use two layers of tests.

Small unit tests:

- one invariant
- one rule
- one edge case

Protocol path tests:

- real election
- real AppendEntries
- real response handling
- real commit update
- real application

Examples of the path style:

```text
start_election()
    -> handle_request_vote_response()
    -> append_entry()
    -> build_append_entries()
    -> follower.handle_append_entries()
    -> leader.handle_append_entries_response()
    -> commit_index advances
    -> state machine applies
```

This gives both precise failures and confidence in the connected flow.

**Current test coverage**

Term and state:

- term starts from zero
- term increments correctly
- term ordering works
- log index starts from zero
- log index increments correctly
- persistent state can be constructed
- volatile state can be constructed

Log:

- append returns the expected index
- entries keep their term
- last index and last term are correct
- term lookup works
- conflicting suffix can be truncated
- AppendEntries prefix can be checked
- leader entries are built in the expected order

RequestVote:

- older term is rejected
- newer term updates the node
- vote is recorded
- second different candidate cannot get the same term's vote
- same candidate can be handled correctly
- candidate log freshness is checked
- granting a vote resets election timing

Election:

- follower does not time out too early
- follower starts an election after timeout
- candidate starts another election after timeout
- each new election gets a higher term
- candidate can remain candidate without enough votes
- majority votes make the candidate leader
- current-term AppendEntries makes the candidate step down

AppendEntries:

- newer term updates the follower
- older term is rejected
- missing previous log entry is rejected
- new entries are appended
- conflicting entries are replaced
- heartbeat succeeds
- heartbeat does not add log entries
- heartbeat resets election timing
- stale heartbeat does not reset election timing
- follower does not commit beyond its local log
- follower commit index does not move backwards

Replication:

- follower progress starts correctly
- successful replication advances progress
- stale success cannot move `match_index` backwards
- failure moves `next_index` backwards
- failure cannot move `next_index` below `match_index`
- each follower gets its own progress state
- unknown follower handling is safe
- AppendEntries starts at `next_index`
- replication retry works after conflict
- heartbeat does not advance `match_index`

Commit:

- leader counts itself in the majority
- majority replication advances commit index
- no majority means no commit
- commit index never moves backwards
- older-term entry is not directly committed by the new leader
- commit triggers state-machine application

State machine:

- committed entries are applied in order
- already-applied entries are not applied again
- `last_applied` only advances after success
- failed application leaves `last_applied` unchanged
- failed entries can be retried

Transport:

- messages can be queued
- messages can be inspected
- messages can be delivered
- targeted delivery works
- messages can be dropped
- messages can be reordered
- duplicate delivery can be tested
- pending message count can be inspected

Restart:

- persistent state can be extracted
- a node can be rebuilt from it
- term survives restart
- vote survives restart
- log survives restart
- restarted follower can continue replication

**What the tests changed in the design**

The tests did more than check the implementation. Several tests changed the implementation itself.

Examples:

- stale AppendEntries showed that term validation must happen before timer reset
- follower response handling showed that remote progress should come from the response itself
- the initial heartbeat change showed that old tests must be updated when the protocol contract changes
- state-machine failure showed that commit and apply are separate states
- replication tests showed why `next_index` and `match_index` must stay separate
- restart tests showed why persistent and volatile state should stay separate
- transport tests showed why the network should be controllable by the test harness

This is the kind of feedback a good test suite should provide.

A useful test should answer:

```text
What rule does this prove?
What would be unsafe if this test failed?
```

**Logging**

Logging is part of the implementation, not something to add at the end.

Current rough levels:

- `ERROR` - something is broken
- `WARN` - something unexpected happened
- `INFO` - important protocol event
- `DEBUG` - useful implementation detail
- `TRACE` - very detailed execution

Useful events include:

- term changed
- leader elected
- leader stepped down
- command appended
- commit index advanced
- log conflict detected
- entry applied
- heartbeat generated
- heartbeat rejected

Structured fields are useful because they let several node logs be compared without losing the event meaning.

**Deterministic failures are the next important step**

The current transport already gives the foundation for controlled failure tests.

The longer-term test model is:

```text
seed = 12345

start cluster

elect leader

drop message

delay message

duplicate message

partition node 3

crash node 4
restart node 4

heal partition

deliver remaining messages

check invariants
```

The same seed and event sequence should reproduce the same failure.

That is much better than a distributed test that fails once every few hundred runs because of timing.

**Why not optimize the data structures yet?**

Because correctness is still the main problem being solved.

The current order should be:

1. correctness
2. focused tests
3. deterministic failure tests
4. measurement
5. optimization

A more complex data structure is not automatically a better design if it makes the protocol harder to reason about.

**Why not add abstraction everywhere?**

An abstraction should remove real complexity.

Before adding a new layer, ask:

- What problem does this solve?
- What protocol rule becomes easier to express?
- What test becomes easier to write?
- Does this reduce duplication or just move it somewhere else?

The `MessageId` decision is a good example. It was considered, but there was no current correctness problem that required it, so it was left out.

Keep the design simple until a real need appears.

**A few questions worth remembering**

Q: Why not let the leader commit whenever a majority has the entry?

A: Because majority alone is not the full Raft commit rule. The current-term restriction matters for the entry being directly committed by the leader. Without that check, an implementation can look correct on simple replication tests and still violate an important safety rule.

Q: Why not use one `progress` value instead of `next_index` and `match_index`?

A: Because the two values answer different questions. `next_index` is a retry cursor. `match_index` is confirmed replication progress. Combining them makes both replication and commitment harder to reason about.

Q: Why not reset the election timer as soon as any AppendEntries arrives?

A: Because an old packet is not proof that a valid leader is still active. Check the term and log consistency first. Only accepted leader activity should reset the election timer.

Q: Why keep commit and state-machine application separate?

A: Because a committed entry and an applied entry are not the same state. The commit index can move forward even when the state machine fails to apply the next command. `last_applied` must show the actual application boundary.

Q: Why test duplicate and reordered messages when Raft already has terms?

A: Because terms solve only part of the problem. Messages can still arrive late, twice, or in a different order. The implementation needs monotonic progress and safe handling of stale messages as well as term checks.

Q: Why use an in-memory transport instead of starting with real sockets?

A: Because the first job is to prove Raft behavior, not network behavior. A controllable transport makes the failure sequence explicit and repeatable. Real sockets can be added later without changing the core protocol rules.

**Current design shape**

```text
RaftNode
  |
  +-- PersistentState
  |     +-- current_term
  |     +-- voted_for
  |     +-- log
  |
  +-- VolatileState
  |     +-- commit_index
  |     +-- last_applied
  |
  +-- ElectionState
  |
  +-- LeaderState
  |     +-- ReplicationState
  |           +-- next_index
  |           +-- match_index
  |
  +-- election timer
  +-- heartbeat timer
  +-- state machine

Outside the node:

  transport
  test cluster
  persistent storage
  real network
  application state machine
```

This keeps the protocol at the center and lets the surrounding pieces be replaced or tested independently.

**The main design rule to carry forward**

For a system with many moving parts:

1. write down the important states
2. write down the important invariants
3. make each invariant visible in code
4. write a small test for each important rule
5. connect the pieces gradually
6. add failure cases
7. add restart cases
8. test combinations of failures

For distributed systems, the happy path is only the beginning.

The useful questions are:

- What happens when a message is late?
- What happens when it is duplicated?
- What happens when it is dropped?
- What happens when a node crashes?
- What state survives restart?
- What happens with an old term?
- What happens with a conflicting log?
- What happens when the state machine fails?
- What happens when two protocol events arrive in an unexpected order?

The goal is to end up with a collection of small rules that are easy to explain, easy to test, and easy to reason about.


### References

The main reference for the implementation is the Raft paper.

- [Raft: In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- [My Raft notes](https://github.com/souraavv/whitepapers-and-books/blob/main/whitepapers/raft.md)
