# Wikipedia edit-stream classifier on Redpanda

Ingests the live Wikipedia recent-changes firehose, classifies each edit with an
LLM, and lands clean, queryable records in Postgres — all wired with **Redpanda
Connect** and brought up with a single `docker compose up`.

```
Wikipedia SSE ──▶ [ingest]  ──▶  Redpanda topic  ──▶  [enrich / "agent"]  ──▶  Postgres ──▶ SQL views
  (firehose)      model-free       wiki.edits         classify→gate→diff       UPSERT
                  filter+project                       (the multi-step loop)
```

## Why two pipelines with a topic in the middle

This is the central design decision, so it goes first.

The firehose arrives at ~tens of events/sec and is cheap to handle. LLM
inference is slow and CPU-bound. If one pipeline did both, the slow model calls
would throttle ingestion and we'd drop edits under load.

So the work is split into two Connect pipelines joined by a **Redpanda topic**:

- **`ingest`** is deterministic and model-free. It drains the SSE firehose,
  filters hard, projects each event to a compact record, and writes to the
  `wiki.edits` topic at wire speed.
- **`enrich`** is the slow consumer. It reads the topic at its own pace and does
  the LLM work.

**The topic is the backpressure boundary.** Ingestion never blocks on the model;
the topic absorbs bursts and the enricher drains when it can. This is the
classic *synchronous-LLM-call vs. asynchronous-worker* tradeoff made physical —
and it's the reason to split rather than mutate inline.

## The agent loop (inside `enrich`)

Not one giant "classify everything" prompt — a real topology that spends tokens
deliberately:

1. **Cheap pre-filter, no model** (Bloblang). Drop bots, drop non-main
   namespaces, drop edits whose byte-delta is below `MIN_BYTE_DELTA`. *Filter
   before you spend a token* — this is where the data-sense lives.
2. **Seed a default label** (`unknown`) before any model call, so a model
   failure lands as a **row**, not as silence.
3. **Classify** via a `branch` processor → `{label, confidence}`. A *branch*
   (not a mutation) so the original message survives the model round-trip.
4. **Confidence gate** (`switch`). If `confidence < CONFIDENCE_THRESHOLD` **or**
   the label is `vandalism`/`spam`, run a **second pass**: fetch the actual diff
   from the MediaWiki `compare` API and re-classify with real content in hand.
   This is the multi-step loop, and you can point to exactly where the extra
   token spend goes and why.
5. **Defend against dirty JSON**, normalize to the label enum, and **UPSERT**
   into Postgres keyed on the new revision id — so a low-confidence first pass is
   *corrected* by the enriched second pass instead of duplicated.

## Running it

Prerequisites: Docker + Docker Compose. Nothing else — the local model needs no
API key.

```bash
cp .env.example .env
docker compose up --build
```

On first boot the `ollama` service pulls the model (`llama3.2:3b`, ~2 GB) once;
a one-shot `model-puller` service blocks the enricher until it's ready. Give it a
few minutes the first time. After that, edits start flowing into Postgres.

### See the results

```bash
# Label distribution
docker compose exec postgres psql -U wiki -d wiki -c 'SELECT * FROM edit_label_summary;'

# The high-signal feed a moderator would watch
docker compose exec postgres psql -U wiki -d wiki -c 'SELECT * FROM recent_flagged;'

# Raw rows, newest first
docker compose exec postgres psql -U wiki -d wiki \
  -c 'SELECT title, label, confidence, enriched, byte_delta FROM edits ORDER BY updated_at DESC LIMIT 20;'
```

## Local model vs. hosted (drop-in)

The default is **local Ollama** so the project is self-contained and free to
grade. CPU inference is the price — it's deliberately slow, which is exactly why
the topic-buffered async design matters.

For a fast, snappy demo, swap the enricher to the hosted variant:

- Set `OPENAI_API_KEY` in `.env`.
- Point the `connect-enrich` service at `connect/enrich.hosted.yaml` (one line in
  `docker-compose.yml`, noted there).

Same topology, same agent loop — only the model call changes. The local default
also accepts an external server: set `OLLAMA_SERVER=http://host.docker.internal:11434`
and run Ollama on the host to get Metal/GPU acceleration without changing the design.

## Tradeoffs

**Branch vs. mutation (for the model calls).** Every LLM call is wrapped in a
`branch` whose `request_map` extracts just the fields the model needs and whose
`result_map` merges the answer back. A plain mutation processor would overwrite
the in-flight message with the model's raw (and sometimes malformed) output,
losing the original edit. Branching costs a little more config but keeps the
payload intact and makes each model call an isolated, testable unit. The cost:
two mapping hops per call to reason about.

**One classification call vs. a multi-step loop.** A single classify call is
cheaper and simpler, but it's blind to edits where the comment lies or is empty —
exactly the vandalism/spam cases that matter most. The confidence-gated second
pass fetches the real diff only when the model is unsure or has flagged
something dangerous, so the expensive path runs on the minority of edits that
justify it. The cost: added latency and an external API dependency on the
enrichment path, and a second failure mode to defend.

**The topic split (bonus).** Covered above. The cost of the split is operational
surface — two pipelines and a topic to run instead of one process — bought in
exchange for backpressure isolation and independent scaling of ingest vs. infer.

**Dedup via UPSERT idempotency.** Rather than a dedup cache before the model, the
Postgres sink UPSERTs on the revision id, so retries and the two-pass design
converge on one row for free. A pre-model cache would save tokens on true
duplicates; for this stream the simpler idempotent sink is the better trade, and
the cache is the obvious next optimization if token cost became the constraint.

## Honest notes / failure modes

- **First-boot model pull** dominates startup; the `model-puller` gate prevents
  the enricher from failing fast against a model that isn't there yet.
- **Small-model JSON is dirty.** The enricher parses defensively and falls back
  to the seeded `unknown` label rather than dropping the row.
- **CPU inference is slow.** Expect seconds per edit locally; that's the design
  working as intended (the topic buffers), not a bug. Use the hosted variant for
  a fast demo.
- The label taxonomy column has **no DB CHECK constraint** on purpose — an
  unexpected label should be stored and inspectable, not rejected into silence.

## Layout

```
docker-compose.yml        # redpanda, postgres, ollama, model-puller, connect-ingest, connect-enrich
connect/ingest.yaml       # SSE firehose -> filter/project -> topic  (no model)
connect/enrich.yaml       # topic -> agent loop -> Postgres          (local Ollama)
connect/enrich.hosted.yaml# drop-in hosted (OpenAI) variant of enrich
sql/schema.sql            # edits table + analytics views (auto-loaded by postgres)
.env.example              # copy to .env; local-model defaults need no API key
```
