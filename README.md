# Wikipedia Content Moderation AI Pipeline

Ingests the live Wikipedia recent-changes firehose, classifies each edit with an LLM, and lands clean, queryable records in Postgres — all wired with **Redpanda Connect** and brought up with a single `docker compose up`.

![Pipeline diagram](image.png)

## Quickstart

```bash
cp .env.example .env
docker compose up --build
```

First boot pulls the model (`llama3.2:3b`, ~2 GB) once — give it a few minutes.
Then everything is live at:

| What | Where |
|------|-------|
| **Grafana** dashboard — pipeline metrics **and** the classified-edit feed, no login | http://localhost:3000 |
| **Prometheus** | http://localhost:9090 |
| **Postgres** (results) | `localhost:5432` — `psql -U wiki -d wiki` |
| **Redpanda** Kafka API / admin API | `localhost:19092` / `localhost:9644` |
| **Ollama** model server | http://localhost:11434 |)

**To see the results, two ways:**

- **Visual** — open the Grafana dashboard at http://localhost:3000 (no login). Below the throughput/lag metrics, a *Classified content* section reads Postgres directly and shows the label distribution, the flagged vandalism/spam feed a moderator would watch, and a live newest-first table of every classified edit.
- **CLI** — query the store directly:

```bash
# Peek at classified edits once data is flowing
docker compose exec postgres psql -U wiki -d wiki -c 'SELECT * FROM edit_label_summary;'
```

## Pipeline Design

The ingest firehose arrives at ~50 events/sec and is cheap to handle. LLM inference is slow. If one pipeline did both, the slow model calls would throttle ingestion.

So the work is split into two Connect pipelines joined by a **Redpanda topic**:

- **`ingest`** is deterministic and model-free. It drains the SSE firehose, filters on existing attributes, projects each event to a compact record, and writes to the `wiki.edits` topic at wire speed.
- **`enrich`** is the slow consumer. It reads the topic at its own pace and does the LLM work.

**The topic is the backpressure boundary.** Ingestion never blocks on the model; the topic absorbs bursts and the enricher drains when it can. This is the *synchronous-LLM-call vs. asynchronous-worker* tradeoff — and it's the reason to split rather than mutate inline.

## The agent loop (inside `enrich`)

1. **Seed a default label** (`unknown`) before any model call, so a model failure lands as a **row**, not as silence.

2. **Classify** via a `branch` processor → `{label, confidence}`. A *branch* (not a mutation) so the original message survives the model round-trip.

3. **Confidence gate** (`switch`). If `confidence < CONFIDENCE_THRESHOLD` **or** the label is `vandalism`/`spam`, run a **second pass**: fetch the actual diff from the MediaWiki `compare` API and re-classify with real content in hand.  This is the multi-step loop, and you can point to exactly where the extra token spend goes and why.

4. **Defend against dirty JSON**, normalize to the label enum, and **UPSERT** into Postgres keyed on the new revision id — so a low-confidence first pass is *corrected* by the enriched second pass instead of duplicated.

## Tradeoffs

### 1. Synchronous LLM in the pipeline vs. an async worker reading from a topic

**I put the LLM behind a topic-backed worker instead of calling it on the firehose path.** 

The ingest side is deliberately boring: parse SSE, filter,
project, produce to `wiki.edits`. The enricher is allowed to be slow because it is just a consumer. That means a cold local model, a long inference, or a burst of edits creates **consumer lag**, not dropped firehose events.

A synchronous inline call would be simpler: one pipeline, fewer containers, less state to reason about, and lower latency when the model is fast. But it couples the fastest part of the system (stream ingestion) to the slowest and least predictable part (LLM inference). In this project that coupling is especially bad because the default model is local CPU inference; the firehose can keep arriving while the model is still thinking about one edit.

The cost of the async split is operational surface. There is a topic to run, consumer lag to watch, at-least-once delivery to tolerate, and idempotency to consider. 

**When I'd flip:** use the synchronous path for a tiny batch job, a low-volume webhook, or a classifier that must reject/accept an event before the caller continues. For a live stream with bursty input and slow models, the topic boundary is the better failure mode.

### 2. One classification call vs. a multi-step reasoning loop

**I run a confidence-gated two-pass loop, not one prompt.** The first pass classifies on cheap metadata only (title, comment, byte-delta). A `switch` then gates: only edits the model was unsure about (`confidence < CONFIDENCE_THRESHOLD`) **or** flagged as `vandalism`/`spam` earn a second pass that fetches the *actual* diff from the MediaWiki `compare` API and re-classifies with real content in hand. The expensive path runs on the minority of edits that justify it, and the `enriched` flag records which rows got it.

A single classify call is cheaper, simpler, and has one failure mode instead of two. But it's structurally blind to the cases that matter most here: an edit whose comment is empty or actively lies ("fixed typo" on a content blanking) is exactly where vandalism hides, and metadata alone can't see it. The second pass is where the judgment lives — it spends tokens *only* where the first pass admits
uncertainty or raises a red flag.

**When I'd flip:** drop back to one call if the data were self-describing (a source where the summary reliably matches the change), or if the diff-fetch tail latency / MediaWiki rate limits outweighed the accuracy gain. I'd go the *other* direction — add a third pass or a human-review queue — if false negatives on vandalism carried real cost. 

### Other decisions, briefly

- **Branch, not mutation, for every model call** — `request_map`/`result_map` graft the answer onto the original message instead of overwriting the payload with the model's raw (often malformed) output. Near-default once you've been burned once.
- **Dedup via UPSERT, not a pre-model cache** — the sink UPSERTs on `rev_new`, so
  retries and the two-pass design converge on one row for free; a pre-model cache
  is the obvious next step *if* token cost ever became the binding constraint.
- **One topic + label column, not topic-per-label** — labels are an analytics
  dimension here (a `WHERE label IN (...)` view), not a routing destination;
  topic-per-label would only pay off if downstream consumers subscribed per class.

## What surprised me

Three things I didn't expect going in:

- **The competency of the 3B model at classification.** I budgeted the second pass as a *correctness* crutch for a weak local model — fetch the real diff because the small model can't be trusted on metadata alone. In practice `llama3.2:3b` handles the clear-cut cases (obvious vandalism, plain content edits) reasonably well on title/comment/byte-delta alone — better than I'd assumed a model that small would; the second pass earns its keep on the genuinely ambiguous edits rather than as a blanket safety net. The confidence gate ended up looking less like a workaround and more like the actual design.

- **How much junk is in the raw firehose.** I expected to be classifying *edits*; a lot of what arrives is bots, automated reverts, and outright spam. The `ingest` filter/project step turned out to be doing more real work than the model — without it the enricher would burn most of its inference budget on traffic no moderator would ever look at. The signal-to-noise ratio of the source reshaped where the effort went.

- **How far Grafana got for free.** I added it expecting throughput/lag charts and nothing more. Pointing a second datasource straight at Postgres turned it into the actual *product* surface — the flagged vandalism/spam feed and the live classified-edit table a moderator would watch — with zero bespoke UI. The monitoring tool quietly became the demo.

(The boring fourth surprise: how much of the "agent" is plumbing, not prompting. The prompts are a dozen lines; the defensive scaffolding around them — SSE prefix stripping, first-`{...}` extraction, seeding `unknown` before the call, UPSERT for correction — is the actual job. Expected, but worth stating.)

## Where this breaks in production / failure modes

- **Failure Catching** Needs stronger retries on failures - for example on Wikimedia compare API errors aren't handled.  This would break the rest of the pipeline.

- **Small-model JSON is dirty.**  Using regex to parse JSON from model is error prone.   Needs rigorous model testing to catch edge cases.

- **CPU inference is slow.**  Not enough partitions/consumers.  Need to scale consumers to match ingest throughput, or filter harder on the ingress side.

- **The MediaWiki `compare` API is an unthrottled external dependency** on the hot path. At scale the second pass would hit rate limits and add tail latency; it needs a cache, a concurrency cap, and a timeout/fallback to the first-pass label.

- **No dead-letter path.** All error paths fail in the same `unkown` state.  Need separate topics or labels to query data.

- **Infra is all in dev mode**; infrastructure not set up for prod.  For example, 1 node RedPanda topic, no SSL, `latest` images eveerywhere, etc.

## Layout

```
docker-compose.yml        # redpanda, postgres, ollama, model-puller, connect-ingest, connect-enrich
connect/ingest.yaml       # SSE firehose -> filter/project -> topic  (no model)
connect/enrich.yaml       # topic -> agent loop -> Postgres          (local Ollama)
connect/enrich.hosted.yaml# drop-in hosted (OpenAI) variant of enrich
sql/schema.sql            # edits table + analytics views (auto-loaded by postgres)
monitoring/               # prometheus scrape config + auto-provisioned Grafana
                          #   datasources (Prometheus + Postgres) and dashboard
.env.example              # copy to .env; local-model defaults need no API key
```

## Running it

Prerequisites: Docker + Docker Compose. Nothing else — the local model needs no API key.

```bash
cp .env.example .env
docker compose up --build
```

On first boot the `ollama` service pulls the model (`llama3.2:3b`, ~2 GB) once;
a one-shot `model-puller` service blocks the enricher until it's ready. Give it a
few minutes the first time. After that, edits start flowing into Postgres.

### See the results

The fastest look is the **Grafana dashboard** (http://localhost:3000, no login): the
*Classified content* section shows the label distribution, the flagged vandalism/spam
feed, and a live table of recent edits, straight from Postgres. Or query the store
directly:

```bash
# Label distribution
docker compose exec postgres psql -U wiki -d wiki -c 'SELECT * FROM edit_label_summary;'

# The high-signal feed a moderator would watch
docker compose exec postgres psql -U wiki -d wiki -c 'SELECT * FROM recent_flagged;'

# Raw rows, newest first
docker compose exec postgres psql -U wiki -d wiki \
  -c 'SELECT title, label, confidence, enriched, byte_delta FROM edits ORDER BY updated_at DESC LIMIT 20;'
```

The default is **local Ollama** so the project is self-contained and free to grade. CPU inference is the price — it's deliberately slow, which is exactly why the topic-buffered async design matters.

For a fast, snappy demo, swap the enricher to the hosted variant:

- Set `OPENAI_API_KEY` in `.env`.
- Point the `connect-enrich` service at `connect/enrich.hosted.yaml` (one line in
  `docker-compose.yml`, noted there).

Same topology, same agent loop — only the model call changes. The local default
also accepts an external server: set `OLLAMA_SERVER=http://host.docker.internal:11434`
and run Ollama on the host to get Metal/GPU acceleration without changing the design.