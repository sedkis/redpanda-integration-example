# Wikipedia Content Moderation AI Pipeline

A near-real-time moderation pipeline over Wikipedia's edit firehose. Each human edit to an article is classified — `vandalism`, `spam`, `substantive_edit`, `minor_edit`, `revert`, `other` — with a confidence score, and the flagged ones surface on a live dashboard.

**a multi-pass (cheap-first) classification example sitting behind a topic boundary.** Ingest filters hard and never touches a model; a Redpanda topic absorbs the burst; a slow LLM enricher drains it at its own pace and only spends extra tokens on the edits that actually look risky.

![Pipeline diagram](image.png)

## Pipeline Design

- **[`ingest`](connect/ingest.yaml)** is deterministic and model-free. It drains the SSE firehose, filters on field attributes (dropping bot traffic, non-article namespaces, and trivial sub-50-byte edits), projects each event to a compact record, and writes to the `wiki.edits` topic at near wire speed.
- **[`enrich`](connect/enrich.yaml)** is the slow consumer. It reads the topic at its own pace, feeds each edit to an LLM that assigns a `label` and a confidence score, and writes the result to Postgres.

**The topic is the backpressure boundary.** Ingestion never blocks on the model; the topic absorbs bursts and the enricher drains when it can. Everything starts automatically on `docker compose up`.

**What the configs handle beyond the happy path**

- **Non-JSON heartbeats.** The SSE stream interleaves `:ok` heartbeats and frame metadata with the `data:` payloads. Parse is guarded with `.catch(deleted())` so a single non-JSON line is dropped, not passed downstream as a raw string that breaks everything after it.
- **Filter before the model.** Bots, non-main namespaces, non-edits, and below-floor byte deltas are dropped at ingest — the model never sees them.
- **`branch`, not mutation, for the LLM.** The model round-trips on a projected prompt and the result is grafted back, so the original edit record survives enrichment instead of being overwritten by the model response.
- **Fail-as-a-row, never silence.** The enricher seeds `label=unknown` *before* the first model call, so a model or parse failure lands a visible row instead of vanishing.
- **Dirty-JSON defense.** Small models wrap JSON in prose and fences; the outermost `{...}` is extracted, parsed with a fallback, and the label enum is normalized (`lowercase().trim()`).
- **UPSERT sink.** Cold-start LLM failures on the first poll land unclassified, then a later pass fixes them in place — `ON CONFLICT (rev_new) DO UPDATE`, gated so a better/enriched result overwrites a worse one and never the reverse.

## Quickstart

```bash
cp .env.example .env
docker compose up --build
```

First boot pulls the model (`llama3.2:3b`, ~2 GB) once — give it a few minutes.

## Results / Viewing Data

Everything is visible in a single Grafana dashboard at **http://localhost:3000** (anonymous access, no login), streaming live:

- **Pipeline health** — ingest arrival rate vs. enrich drain rate, consumer lag, and ops/sec on each side, so you can see at a glance whether the enricher is keeping up with the firehose.
- **Classified edits** — the actual moderation output, read straight from Postgres, with the flagged content (`vandalism` / `spam`) broken out into its own tiles at the bottom of the dashboard. That's the part a moderator would actually act on.


## AI Approach — preparing for production

The multi-pass mechanism is described in Tradeoff #1 below. The classification and confidence score in this setup are **not to be trusted** — the project simulates the *shape* of a production cascade (cheap inference first, expensive assessment only when warranted), not a deployable classifier. A few things would have to change before that claim held up:

- **Evals.** Rigorous, measured testing for false negatives and false positives — not the handful of rows I eyeballed.
- **Don't trust user-supplied text.** This setup classifies partly on the edit comment, which makes no sense for a system whose whole job is catching malicious activity. All user-inputted data has to be treated as potentially, deliberately misleading.
- **A grounded confidence signal.** The score is subjective and drifts across models. In production you'd fine-tune a specialized model on thousands of human-labeled diffs and give it an objective rubric to score against, so the number means something consistent.

None of this is hypothetical — Wikimedia runs exactly this kind of specialized classifier in production. [ORES](https://wikitech.wikimedia.org/wiki/ORES) scored edits for years on `revscoring` models trained on thousands of human-labeled diffs, and its LLM-based successor, the [Revert Risk models on Lift Wing](https://www.mediawiki.org/wiki/Machine_Learning), does it today. This project mimics the *shape* of that pipeline; those are what the real thing looks like.

## Tradeoff #1 — Multi-pass vs. single pass

The first pass classifies on cheap metadata only (title, comment, byte-delta). A `switch` then gates: only edits the model was unsure about (`confidence < CONFIDENCE_THRESHOLD`) **or** flagged as `vandalism`/`spam` earn a second pass that fetches the *actual* diff from the MediaWiki `compare` API and re-classifies with real content in hand. The expensive path runs on the minority of edits that justify it, and the `enriched` flag records which rows got it.

A single classify call is cheaper, simpler, and has one failure mode instead of two. But it's structurally blind to the cases that matter most here: an edit whose comment is empty or actively lies ("fixed typo" on a content blanking) is exactly where vandalism hides, and metadata alone can't see it. The second pass is where the judgment lives — it spends tokens *only* where the first pass admits uncertainty or raises a red flag.

**When I'd flip:** drop back to one call if the data were self-describing (a source where the summary reliably matches the change), or if the diff-fetch tail latency / MediaWiki rate limits outweighed the accuracy gain. I'd go the *other* direction — add a third pass or a human-review queue — if false negatives on vandalism carried real cost.

*Shaped by:* [FrugalGPT (Chen, Zaharia & Zou, 2023)](https://arxiv.org/abs/2305.05176) — the cheap-model-first, escalate-on-uncertainty cascade is the same idea applied to cost rather than just accuracy.

## Tradeoff #2 — Synchronous LLM in the pipeline vs. an async worker reading from a topic

**I put the LLM behind a topic-backed worker instead of calling it synchronously on the firehose path.**

The ingest side is static, in-memory filtering. It's fast — very fast. The AI enricher is slow, and its inference time varies randomly. That throughput mismatch was the main reason to separate the pipelines: a topic in the middle handles the backpressure, so the firehose-side never blocks on a model.

There's a second reason. The Redpanda topic is an immutable, replayable log. In the AI world we spend a lot of time playing with models — fine-tuning, testing new ones, replaying evals. Persisting every edit on the topic means "re-run all of it against a new model" is a config change, not a re-ingest.

The cost of the async split is operational surface: a topic to run, consumer lag to watch, at-least-once delivery to tolerate, and idempotency to consider. That's not nothing, but it's well worth it given the benefits.

**When I'd flip:** use the synchronous path for a tiny batch job, a low-volume webhook, or a classifier that must reject/accept an event before the caller continues. For a live stream with bursty input and slow models, the topic boundary is the better failure mode.

*Shaped by:* Jay Kreps, [*The Log*](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) — the replayable, immutable log as the integration point is what makes "re-run every edit against a new model" a config change rather than a re-ingest. Mechanics of the LLM call itself follow Connect's [`branch` processor](https://docs.redpanda.com/redpanda-connect/components/processors/branch/) (request_map builds the prompt, result_map grafts the output back).

## What surprised me

- **The competency of the 3B model at classification.** Even on an M2 Pro / 16 GB, `llama3.2:3b` was fast and capable — ~2s to classify metadata, and relatively accurate. The caveat stands: I only reviewed a handful of rows, and I'd want far more measured data before trusting any model — not just this one — in production.
- **How much junk is in the raw firehose.** I expected to be classifying *edits*; most of what arrives is bot traffic, and only a small fraction of diffs survive the ingest filters.
- **How deep and easy the Grafana integration was.** One of my first questions was "how much is ingest generating vs. enrich consuming — what's the lag?" I plugged in Grafana and had consumer lag, per-pipeline throughput, and ops/sec almost immediately. Being able to query Postgres directly for the results was a nice cherry on top.

## Where this breaks in production / failure modes

- **Failure catching.** Needs stronger retries on failures — for example, errors from the Wikimedia `compare` API aren't handled, and would break the rest of the pipeline.
- **Small-model JSON is dirty.** Using regex to extract JSON from model output is error-prone. A better approach would be a model that guarantees structured JSON through its API.
- **CPU inference is slow.** Not enough partitions/consumers. Consumers need to scale out to match ingest throughput.
- **No dead-letter path.** All error paths collapse into the same `unknown` state. Production wants separate topics or labels — and we'd likely want to *keep* the bot/spam/junk diffs for further analytics instead of dropping them.
- **Infra is all in dev mode.** Single-node Redpanda, no SSL, `latest` images everywhere, no infra sizing — storage limits, CPU estimates, memory profiles, none of it.

## Layout

```
docker-compose.yml         # redpanda, postgres, ollama, model-puller, connect-ingest, connect-enrich
connect/ingest.yaml        # SSE firehose -> filter/project -> topic  (no model)
connect/enrich.yaml        # topic -> agent loop -> Postgres          (local Ollama)
connect/enrich.hosted.yaml # drop-in hosted (OpenAI) variant of enrich
sql/schema.sql             # edits table + analytics views (auto-loaded by postgres)
monitoring/                # prometheus scrape config + auto-provisioned Grafana
                           #   datasources (Prometheus + Postgres) and dashboard
.env.example               # copy to .env; local-model defaults need no API key
```

## Running with a hosted model

The default is **local Ollama** so the project is self-contained and free to grade. CPU inference is the price — it's deliberately slow, which is exactly why the topic-buffered async design matters.

For a fast, snappy demo, swap the enricher to the hosted variant:

- Set `OPENAI_API_KEY` in `.env`.
- Point the `connect-enrich` service at `connect/enrich.hosted.yaml` (one line in `docker-compose.yml`, noted there).

Same topology, same agent loop — only the model call changes. The local default also accepts an external server: set `OLLAMA_SERVER=http://host.docker.internal:11434` and run Ollama on the host to get Metal/GPU acceleration without changing the design.
