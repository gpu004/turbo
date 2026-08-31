# R2 queue benchmark and S3 estimate

Measured August 31, 2026.

## Result

The queue batches correctly, but the current R2 design cannot use its 5 ms commit interval in production.

Cloudflare R2 allows at most one write per second to the same object key. This queue rewrites one `queue.json`, so one queue has a hard ceiling of one durable commit per second. Client throughput can exceed one job per second only by putting many jobs into each commit. Cloudflare says writes above the same-key limit return HTTP `429`.

The local Worker reached a median 789.5 pushes/s with 32 concurrent clients. That is a Miniflare result, not an R2 result. Miniflare did not apply R2's same-key write limit.

A real R2 run could not start. `npx wrangler r2 bucket list` returned Cloudflare API error `10042`:

```text
Please enable R2 through the Cloudflare Dashboard.
```

No bucket was created, no Worker was deployed, and no measured remote R2 latency appears in this report.

## Queue under test

The benchmark exercised the HTTP API and the code in [`src/broker.ts`](src/broker.ts). Each named queue routes to one Durable Object. The broker batches requests, rewrites `queues/<queue>/queue.json`, and acknowledges each request after the conditional R2 `put()` succeeds.

The test payload was 126 to 128 bytes of JSON. Each run used a fresh queue name. Push tests issued 400 requests at fixed concurrency. Claim and completion tests used concurrency 32. Every result below is the median of three runs.

Test host and runtime:

| Item | Value |
| --- | --- |
| Host | Apple M1, arm64 |
| OS | macOS 26.6.2, build 25G83 |
| Node.js | 26.8.1 |
| Wrangler | 4.124.0 |
| Storage | local Miniflare R2 simulator |
| Durable Objects | local workerd simulator |
| Compatibility date used by local workerd | 2026-08-22 |
| Repository base revision | `571c903f3fce897e21645bb636742dc52bdacef2` plus the uncommitted queue implementation |
| Requests per trial | 400 |
| Trials | 3 |

Wrangler request logging remained enabled. These numbers are useful for comparing concurrency levels in this implementation. They are not clean CPU microbenchmarks.

## Local emulator measurements

### Push

| Concurrency | Throughput | p50 | p95 | p99 | Jobs per JSON commit |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 75.4 ops/s | 12.00 ms | 17.61 ms | 25.62 ms | 1.00 |
| 8 | 528.3 ops/s | 12.02 ms | 18.65 ms | 38.62 ms | 7.55 |
| 32 | 789.5 ops/s | 34.00 ms | 51.73 ms | 60.88 ms | 17.39 |
| 128 | 747.7 ops/s | 145.68 ms | 196.64 ms | 197.92 ms | 16.67 |

Concurrency 32 was the best tested point. Concurrency 128 did not improve throughput and raised p50 latency by more than 4x. The broker accumulated more waiting requests without producing larger median batches.

### Claim and completion

| Operation | Concurrency | Throughput | p50 | p95 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Claim one job | 32 | 776.6 ops/s | 36.05 ms | 55.25 ms | 56.71 ms |
| Complete one job | 32 | 844.7 ops/s | 32.19 ms | 54.26 ms | 57.76 ms |

All claimed jobs had distinct lease tokens. Completion left zero jobs in each lifecycle test queue.

### What the local run proves

- Concurrent clients share group commits.
- Claims do not hand the same job to two workers in this test.
- The HTTP layer, Durable Object broker, conditional write path, heartbeat model, and completion path run together in workerd.
- More concurrency eventually increases waiting time without increasing throughput.

It does not measure Cloudflare network latency, remote Durable Object placement, R2 durability latency, R2 throttling, or tail behavior during retries and failover.

## R2 production bound

Cloudflare documents a [maximum of one write per second to the same object name](https://developers.cloudflare.com/r2/platform/limits/). That limit dominates this design.

If a commit contains `B` mutations, the best-case queue throughput bound is:

```text
R2 queue throughput <= B jobs/second
```

Examples:

| Jobs per commit | Maximum durable job rate per queue shard | Commits needed for 1 million jobs |
| ---: | ---: | ---: |
| 1 | 1 job/s | 1,000,000 |
| 10 | 10 jobs/s | 100,000 |
| 100 | 100 jobs/s | 10,000 |
| 1,000 | 1,000 jobs/s | 1,000 |

This is a ceiling, not a latency measurement. Serialization time, object size, R2 `put()` latency, retries, and Durable Object load can reduce it.

The current `QUEUE_BATCH_WINDOW_MS=5` setting is unsafe for remote R2. It allows the broker to begin another commit far sooner than the one-write-per-second limit. The broker also lacks a specific `429` retry path. Under steady traffic, requests can fail instead of waiting for the next legal commit.

To retain the one-file design on R2, the broker needs all of the following:

- a commit scheduler that starts no more than one same-key write per second
- bounded retry with jitter for `429` and transient R2 errors
- enough pending-request capacity for one-second batches
- multiple queue names when one shard cannot hold the arrival rate

The 1-second commit cadence adds batching delay. With steady arrivals, the scheduling delay alone averages about 500 ms and approaches 1 second at the tail, before the R2 write time. Remote R2 latency remains unmeasured.

## S3 comparison

The local napkin-math checkout is `sirupsen/napkin-math` at commit [`aae5832`](https://github.com/sirupsen/napkin-math/tree/aae5832fc5d7c6881b7a78f756e8863ec86c428b). Its object-storage table uses these rounded planning numbers:

| Operation | Napkin latency | Napkin throughput |
| --- | ---: | ---: |
| Conditional GET returning 304 | 30 ms | Not given |
| Single-connection GET | 80 ms | 100 MiB/s |
| LIST | 100 ms | Not given |
| Single-connection PUT | 200 ms | 100 MiB/s |

The same repository reports a March 2026 S3 measurement of about 95 MiB/s for a same-region 1 GiB single-stream GET, 2.0 GiB/s for concurrent range GETs, and 1.8 to 1.9 GiB/s for multipart PUTs. Those large-object throughput measurements do not predict this queue's small conditional overwrite latency. The repository calls its small-object latency figures rough heuristics. See the [object-storage numbers and provenance](https://github.com/sirupsen/napkin-math/blob/aae5832fc5d7c6881b7a78f756e8863ec86c428b/README.md#numbers).

AWS publishes a broader range. Its S3 performance guide says small-object latency is roughly [100 to 200 ms](https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html). It also states that one prefix supports at least 3,500 write requests/s and 5,500 read requests/s after scaling. That prefix limit does not make this queue commit 3,500 times/s. A CAS broker must serialize updates to one key, so the 100 to 200 ms request latency implies roughly 5 to 10 sequential commits/s before application overhead. The 200 ms napkin figure gives the more conservative estimate of 5 commits/s.

S3 now supports `If-Match` on `PutObject`, so the R2 CAS algorithm has a direct S3 equivalent. S3 also provides strong read-after-write consistency for object overwrites. See AWS's [conditional write](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html) and [consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel) documentation.

### Estimated queue behavior

| Dimension | R2 version | S3 port |
| --- | --- | --- |
| Evidence | Published hard limit, remote latency unmeasured | Napkin estimate plus AWS guidance |
| CAS mechanism | R2 `onlyIf` with ETag | S3 `If-Match` with ETag |
| Strong overwrite consistency | Yes | Yes |
| Same-key durable commit rate | At most 1/s | About 5/s using the 200 ms napkin PUT estimate |
| Small conditional PUT latency | Unmeasured | About 100 to 200 ms |
| Job rate with 100 jobs per commit | At most 100 jobs/s | About 500 jobs/s |
| Job rate with 1,000 jobs per commit | At most 1,000 jobs/s | About 5,000 jobs/s |
| Main scaling method | More queue keys | More queue keys and prefixes |

The S3 row is an estimate, not a benchmark of this TypeScript service. A fair live comparison would place the S3 broker on EC2 in the bucket's region and the R2 broker in a deployed Worker. Running both clients from this Mac would mostly measure internet distance.

## Request cost

The hot broker caches the last queue snapshot, so the steady-state object-storage cost is one write per group commit. Cold starts add a read. The table below prices only object-storage requests for 1 million jobs and ignores free tiers, Worker or EC2 compute, Durable Object requests, logging, and failed retries.

R2 Standard charges $4.50 per million Class A operations. S3 Standard PUT requests are commonly priced at $0.005 per 1,000 requests in US regions, which is $5 per million. Confirm the chosen AWS region on the [S3 pricing page](https://aws.amazon.com/s3/pricing/). Current R2 prices and its free tier are on the [R2 pricing page](https://developers.cloudflare.com/r2/pricing/).

| Jobs per commit | Commits | R2 write cost | S3 write cost estimate |
| ---: | ---: | ---: | ---: |
| 1 | 1,000,000 | $4.5000 | $5.0000 |
| 10 | 100,000 | $0.4500 | $0.5000 |
| 100 | 10,000 | $0.0450 | $0.0500 |
| 1,000 | 1,000 | $0.0045 | $0.0050 |

Request pricing is nearly tied. Commit latency, the R2 same-key cap, and broker compute matter more than object request cost for this queue.

## Rewrite amplification and capacity

The 400-job push queue occupied 97,676 bytes, or 244.19 bytes per pending job for this payload. At that density, the configured 8 MiB cap holds about 34,352 pending jobs. Leased jobs are larger because they add a worker ID, lease token, heartbeat, and expiry time, so the real claimed-job capacity is lower.

The queue rewrites old jobs on every commit. To grow an empty queue to 10,000 pending jobs:

| Batch size | Commits | Approximate bytes written | Write bytes per accepted job |
| ---: | ---: | ---: | ---: |
| 100 jobs | 100 | 117.6 MiB | 12.3 KiB |
| 1,000 jobs | 10 | 12.8 MiB | 1.3 KiB |

Larger batches improve request cost and rewrite amplification. On R2 they also raise the only useful throughput lever available to a single key. The tradeoff is more waiting time and more requests lost together if a commit fails.

## Decision

Do not deploy the current 5 ms commit loop against R2 unchanged.

For a low-rate notification queue, keep the one-file design and pace commits at one per second. Make `429` retryable and use a one-second batch. For higher rates, shard across queue files. If strict global FIFO across all jobs matters more than the one-file constraint, R2 is the wrong hot coordination store for this implementation. Durable Object SQLite or Cloudflare Queues can hold the live queue while R2 stores snapshots or an append-only audit log.

## Remote benchmark still needed

After R2 is enabled on the Cloudflare account, run a separate remote matrix with fresh queue names:

1. Deploy the Worker and bind a real `turbo-queues` bucket.
2. Test the current 5 ms loop to record the first `429` threshold and error behavior.
3. Test a corrected 1-second commit scheduler at client concurrency 1, 8, 32, 128, and 1,000.
4. Record successful operations, `429` responses, retries, jobs per commit, p50, p95, p99, queue bytes, and R2 operations billed.
5. Repeat from at least two client regions and record Durable Object placement.

Until that run exists, this report contains a measured local implementation benchmark, a published R2 throughput bound, and an S3 estimate. It does not claim measured remote R2 performance.
