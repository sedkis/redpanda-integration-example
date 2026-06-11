# Wikipedia Content Moderation AI Pipeline

A content moderation system, built on realtime data incoming from Wikipedia Firehose.  

Receives virtually all diffs across the entirety of Wikipedia, uses AI to identify and flag spam, vandalism, etc.


![Pipeline diagram](image.png)

## Pipeline Design


- **[`ingest`](connect/ingest.yaml)** is deterministic and model-free. It drains the SSE firehose, filters on field attributes (dropping bot traffic), projects each event to a compact record, and writes to the `wiki.edits` topic at near wire speed.
- **[`enrich`](connect/enrich.yaml)** is the slow consumer. It reads the topic at its own pace, feeds each edit to an LLM that assigns a `label` and a confidence score, and writes the result to Postgres.

**The topic is the backpressure boundary.** Ingestion never blocks on the model; the topic absorbs bursts and the enricher drains when it can. Everything starts automatically on `docker compose up`.

## Quickstart

```bash
cp .env.example .env
docker compose up --build
```

First boot pulls the model (`llama3.2:3b`, ~2 GB) once — give it a few minutes.

## Results / Viewing Data

Visit Grafana to see metrics on each pipeline, topic, as well as the actual classified data in Postgres, all streamed in realtime.

http://localhost:3000 - no user/pw.

There are tiles which show flagged content at the bottom of the single Grafana dashboard.

## AI Approach - preparing for production

The multi-pass mechanism is described in Tradeoff #1 below. The classification and confidence score in this setup are **not to be trusted** — the project simulates the *shape* of a production cascade (cheap inference first, expensive assessment only when warranted), not a deployable classifier. A few things would have to change before that claim held up:

- **Evals.** Rigorous, measured testing for false negatives and false positives — not the handful of rows I eyeballed.
- **Don't trust user-supplied text.** This setup classifies partly on the edit comment, which makes no sense for a system whose whole job is catching malicious activity. All user-inputted data has to be treated as potentially, deliberately misleading.
- **A grounded confidence signal.** The score is subjective and drifts across models. In production you'd fine-tune a specialized model on thousands of human-labeled diffs and give it an objective rubric to score against, so the number means something consistent.

## Tradeoff #1 - Multi-pass vs single pass

The first pass classifies on cheap metadata only (title, comment, byte-delta). A `switch` then gates: only edits the model was unsure about (`confidence < CONFIDENCE_THRESHOLD`) **or** flagged as `vandalism`/`spam` earn a second pass that fetches the *actual* diff from the MediaWiki `compare` API and re-classifies with real content in hand. The expensive path runs on the minority of edits that justify it, and the `enriched` flag records which rows got it.

A single classify call is cheaper, simpler, and has one failure mode instead of two. But it's structurally blind to the cases that matter most here: an edit whose comment is empty or actively lies ("fixed typo" on a content blanking) is exactly where vandalism hides, and metadata alone can't see it. The second pass is where the judgment lives — it spends tokens *only* where the first pass admits
uncertainty or raises a red flag.

**When I'd flip:** drop back to one call if the data were self-describing (a source where the summary reliably matches the change), or if the diff-fetch tail latency / MediaWiki rate limits outweighed the accuracy gain. I'd go the *other* direction — add a third pass or a human-review queue — if false negatives on vandalism carried real cost. 

*Shaped by:* [FrugalGPT (Chen, Zaharia & Zou, 2023)](https://arxiv.org/abs/2305.05176) — the cheap-model-first, escalate-on-uncertainty cascade is the same idea applied to cost rather than just accuracy.


### Tradeoff #2 - Synchronous LLM in the pipeline vs. an async worker reading from a topic

**I put the LLM behind a topic-backed worker instead of calling it synchronously on the firehose path**

The ingest side is static, in memory filtering.  It's fast. Very fast.  However, the AI enricher is very slow.  The inference time also varies randomly.   This mismatch in throughput was the main reason to separate the pipelines.  We can have a topic in the middle handle the backpressure. 

In addition, the redpanda topic is an immutable, replayable log.  in the AI world, we spend a lot of time playing with models.  Fine-tuning, testing new ones, replaying evals.  Having all the data persisted in the RedPanda topic allows us to replay and test various models as many times as we want.

The cost of the async split is operational surface. There is a topic to run, consumer lag to watch, at-least-once delivery to tolerate, and idempotency to consider.   That's not nothing, but well worth it given the benefits.

**When I'd flip:** use the synchronous path for a tiny batch job, a low-volume webhook, or a classifier that must reject/accept an event before the caller continues. For a live stream with bursty input and slow models, the topic boundary is the better failure mode.

*Shaped by:* Jay Kreps, [*The Log*](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) — the replayable, immutable log as the integration point is what makes "re-run every edit against a new model" a config change rather than a re-ingest. Mechanics of the LLM call itself follow Connect's [`branch` processor](https://docs.redpanda.com/redpanda-connect/components/processors/branch/) (request_map builds the prompt, result_map grafts the output back).


## What surprised me

Three things I didn't expect going in:

- **The competency of the 3B model at classification.**  The model, even on a m2 pro, 16gb ram, was fairly quick and capable.  I did simple chat at first, and was surprised.  In the classification exercise, it was surprisingly fast and accurate.  It takes an average of 2s to classify metadata in this setup.   Importantly, I only reviewed a handful of rows.  Would need to measure much more data before having confidence to deploy in a real-world scenario.  That goes for any model, not only the 3B one.

- **How much junk is in the raw firehose.** I expected to be classifying *edits*; Most of what arrives is bot traffic.  I estimate only a small fraction of diffs made it past the ingest filters.

- **How deep and simple Grafana integration was.**  I was surprised at how rich the Grafana plumbing is.  

One of my first questions was: "How much data is the ingest generating, and how much is the enrich consuming?".  In other words, what's the lag?

I plugged in Grafana and could see lag, throughput on both pipelines, ops per second, a bunch of other stuff.  It was surprisingly easy to have really rich metrics.

Being able to query Postgres directly to view the results was a nice cherry on top.

## Where this breaks in production / failure modes

- **Failure Catching** Needs stronger retries on failures - for example on Wikimedia compare API errors aren't handled.  This would break the rest of the pipeline.

- **Small-model JSON is dirty.**  Using regex to parse JSON from model is error prone.  Needs a better approach.  Perhaps using a model which is capable of returning JSON through its API interface.

- **CPU inference is slow.**  Not enough partitions/consumers.  Need to scale consumers to match ingest throughput.

- **No dead-letter path.** All error paths fail in the same `unknown` state.  Need separate topics or labels to query data.  in prod, we'd likely want to keep the crap/spam/bot data diffs, instead of simply deleting, so we can run further analytics.

- **Infra is all in dev mode**; infrastructure not set up for prod at all.  For example, 1 node RedPanda topic, no SSL, `latest` images everywhere, no infra sizing, storage limits, cpu estimates, memory profiles, etc etc etc

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

## Running with hosted model

The default is **local Ollama** so the project is self-contained and free to grade. CPU inference is the price — it's deliberately slow, which is exactly why the topic-buffered async design matters.

For a fast, snappy demo, swap the enricher to the hosted variant:

- Set `OPENAI_API_KEY` in `.env`.
- Point the `connect-enrich` service at `connect/enrich.hosted.yaml` (one line in
  `docker-compose.yml`, noted there).

Same topology, same agent loop — only the model call changes. The local default
also accepts an external server: set `OLLAMA_SERVER=http://host.docker.internal:11434`
and run Ollama on the host to get Metal/GPU acceleration without changing the design.