---------------------------- MODULE QueueBroker ----------------------------
(***************************************************************************)
(* The commit protocol in src/broker.ts. A broker keeps a cached snapshot  *)
(* of the R2 queue object, applies a mutation to it, and writes the result *)
(* back with a compare-and-set on the etag. If the write fails, it reloads.*)
(*                                                                         *)
(* Cloudflare routes a queue to one Durable Object, but a restart or a     *)
(* network partition can briefly leave two instances holding snapshots, so *)
(* the spec allows several brokers. The guarantees in readme.md must hold  *)
(* anyway, and they rely on the compare-and-set to do it.                  *)
(*                                                                         *)
(* Each step commits one mutation. test/model.property.test.ts checks that *)
(* a group commit gives the same results as applying its mutations one at  *)
(* a time, which is what makes that simplification sound. FIFO order and  *)
(* idempotency keys are also left to the property tests.                   *)
(* test/broker.test.ts runs the implementation against an in-memory store  *)
(* with the same compare-and-set rule, including a lost race.              *)
(*                                                                         *)
(* A batch that changes nothing is answered from the cached snapshot       *)
(* without contacting R2. That path is not modeled. Its success replies,   *)
(* `alreadyCompleted: true` for a job missing from the snapshot and        *)
(* `duplicate: true` for a key already in it, are only sound while a       *)
(* single broker holds the queue, so `acked` and `done` here count only    *)
(* replies that followed a conditional write.                              *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
  Brokers,       \* Durable Object instances that may hold a snapshot
  Workers,       \* clients that claim jobs
  Jobs,          \* job ids; each is pushed at most once
  LeaseTicks,    \* QUEUE_LEASE_SECONDS, in clock ticks
  MaxTime,       \* bound on the clock, to keep the state space finite
  MaxRevisions,  \* bound on R2 writes, for the same reason
  CompareAndSet  \* FALSE models an unconditional R2 put

VARIABLES
  store,   \* the R2 object: [rev, jobs]
  cache,   \* broker -> the snapshot it last loaded or wrote
  acked,   \* jobs a client was told were pushed
  done,    \* jobs a client was told were completed
  held,    \* worker -> leases it was told it holds
  tokens,  \* lease tokens issued so far; the next token is tokens + 1
  now

vars == <<store, cache, acked, done, held, tokens, now>>

Absent  == [state |-> "absent", token |-> 0, expires |-> 0]
Pending == [state |-> "pending", token |-> 0, expires |-> 0]

Leased(t, e) == [state |-> "leased", token |-> t, expires |-> e]

Lease(j, t, e) == [job |-> j, token |-> t, expires |-> e]

Claimable(r) == r.state = "pending" \/ (r.state = "leased" /\ now >= r.expires)

\* What `findLiveLease` in src/model.ts accepts.
Live(r, l) == r.state = "leased" /\ r.token = l.token /\ now < r.expires

\* Broker b writes `jobs` as the next revision of its snapshot. With
\* CompareAndSet the write succeeds only if R2 still holds that snapshot.
Commit(b, jobs) ==
  /\ CompareAndSet => store.rev = cache[b].rev
  /\ LET next == [rev |-> cache[b].rev + 1, jobs |-> jobs]
     IN  /\ store' = next
         /\ cache' = [cache EXCEPT ![b] = next]

Init ==
  /\ store = [rev |-> 0, jobs |-> [j \in Jobs |-> Absent]]
  /\ cache = [b \in Brokers |-> store]
  /\ acked = {}
  /\ done = {}
  /\ held = [w \in Workers |-> {}]
  /\ tokens = 0
  /\ now = 0

Push(b, j) ==
  /\ j \notin acked
  /\ cache[b].jobs[j] = Absent
  /\ Commit(b, [cache[b].jobs EXCEPT ![j] = Pending])
  /\ acked' = acked \cup {j}
  /\ UNCHANGED <<done, held, tokens, now>>

Claim(b, w, j) ==
  /\ Claimable(cache[b].jobs[j])
  /\ LET t == tokens + 1
         e == now + LeaseTicks
     IN  /\ Commit(b, [cache[b].jobs EXCEPT ![j] = Leased(t, e)])
         /\ tokens' = t
         /\ held' = [held EXCEPT ![w] = @ \cup {Lease(j, t, e)}]
  /\ UNCHANGED <<acked, done, now>>

Heartbeat(b, w, l) ==
  /\ l \in held[w]
  /\ Live(cache[b].jobs[l.job], l)
  /\ LET e == now + LeaseTicks
     IN  /\ Commit(b, [cache[b].jobs EXCEPT ![l.job] = Leased(l.token, e)])
         /\ held' = [held EXCEPT ![w] = (@ \ {l}) \cup {Lease(l.job, l.token, e)}]
  /\ UNCHANGED <<acked, done, tokens, now>>

Complete(b, w, l) ==
  /\ l \in held[w]
  /\ Live(cache[b].jobs[l.job], l)
  /\ Commit(b, [cache[b].jobs EXCEPT ![l.job] = Absent])
  /\ done' = done \cup {l.job}
  /\ held' = [held EXCEPT ![w] = @ \ {l}]
  /\ UNCHANGED <<acked, tokens, now>>

\* A failed compare-and-set, a restart, or `inspect` reloads from R2.
Reload(b) ==
  /\ cache[b] # store
  /\ cache' = [cache EXCEPT ![b] = store]
  /\ UNCHANGED <<store, acked, done, held, tokens, now>>

\* Workers forget a lease once it expires. An expired lease can no longer
\* heartbeat or complete, so dropping it only keeps the state space small.
Tick ==
  /\ now < MaxTime
  /\ now' = now + 1
  /\ held' = [w \in Workers |-> {l \in held[w] : now' < l.expires}]
  /\ UNCHANGED <<store, cache, acked, done, tokens>>

Next ==
  \/ Tick
  \/ \E b \in Brokers :
       \/ Reload(b)
       \/ \E j \in Jobs : Push(b, j)
       \/ \E w \in Workers, j \in Jobs : Claim(b, w, j)
       \/ \E w \in Workers : \E l \in held[w] : Heartbeat(b, w, l) \/ Complete(b, w, l)

Spec == Init /\ [][Next]_vars

\* A heartbeat writes a new revision even when the clock has not moved, as
\* in src/model.ts, so revisions need their own bound.
Bounded == store.rev <= MaxRevisions

\* Brokers, workers, and jobs are interchangeable, so TLC checks one
\* representative of each permutation.
Symmetry == Permutations(Brokers) \cup Permutations(Workers) \cup Permutations(Jobs)

-----------------------------------------------------------------------------
(* Invariants. Each one is a guarantee from readme.md.                     *)

\* An acknowledged push stays in R2 until an acknowledged completion.
NoLostJob == \A j \in acked \ done : store.jobs[j].state # "absent"

\* An acknowledged completion is never undone.
NoResurrection == \A j \in done : store.jobs[j].state = "absent"

\* A lease a worker was given, and has not seen expire, is the one R2 holds.
LeaseHonoured ==
  \A w \in Workers : \A l \in held[w] :
    now < l.expires => Live(store.jobs[l.job], l)

\* At most one worker holds a live lease on a job.
ExclusiveLease ==
  \A j \in Jobs :
    Cardinality({w \in Workers : \E l \in held[w] : l.job = j /\ Live(store.jobs[j], l)}) <= 1

=============================================================================
