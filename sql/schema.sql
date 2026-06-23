-- Wikipedia edit-stream classifier — Postgres schema
-- Loaded automatically by the postgres container on first boot
-- (mounted into /docker-entrypoint-initdb.d).

-- One row per Wikipedia revision. The NEW revision id is the natural,
-- idempotent UPSERT key: re-processing the same edit (a retry, or a
-- replay against a new model) updates the row instead of duplicating it.
CREATE TABLE IF NOT EXISTS edits (
    rev_new      BIGINT PRIMARY KEY,        -- revision.new  (upsert key)
    rev_old      BIGINT,                     -- revision.old  (needed to fetch the diff)
    wiki         TEXT,                       -- e.g. enwiki
    server_name  TEXT,                       -- e.g. en.wikipedia.org
    title        TEXT,
    user_name    TEXT,
    is_bot       BOOLEAN,
    is_anon      BOOLEAN,                    -- anonymous (IP) editor — an UNFORGEABLE trust
                                             -- signal; anon edits are the dominant vandalism source
    edit_type    TEXT,                       -- edit | new | ...
    comment      TEXT,                       -- editor-supplied, ADVERSARY-CONTROLLED, never trusted
    byte_delta   INTEGER,                    -- length.new - length.old (a signal, not a filter)
    byte_neutral BOOLEAN,                    -- byte_delta == 0 with a real diff: the hardest
                                             -- adversarial case (e.g. digit-swap 1897 -> 1107)
    schema_version INTEGER,                  -- record/data-contract version

    -- Model output. `label` is intentionally TEXT with no CHECK constraint:
    -- the pipeline already validates against the enum, and we would rather a
    -- surprising label land as a ROW than have a constraint silently drop it.
    label        TEXT    NOT NULL DEFAULT 'unknown',
    raw_label    TEXT    NOT NULL DEFAULT '',  -- when the model emits a label OUTSIDE our
                                             -- taxonomy, `label` is coerced to 'unknown' and
                                             -- the model's actual output is kept HERE — so a
                                             -- misbehaving model lands in diagnostics, not silence
    confidence   REAL    NOT NULL DEFAULT 0.0,
    enriched     BOOLEAN NOT NULL DEFAULT FALSE,  -- did the classifier (rule or model) produce a
                                             -- verdict, vs a seeded/unreachable row? on replay a
                                             -- classified row beats a seeded one (see UPSERT below)
    comment_diff_mismatch BOOLEAN NOT NULL DEFAULT FALSE,  -- the comment contradicts the diff:
                                             -- a deliberate-deception signal, the highest-value tell
    rule_hit     TEXT    NOT NULL DEFAULT '', -- which deterministic tripwire fired (provenance:
                                             -- rule vs model); '' = no rule, model decided
    review_state TEXT    NOT NULL DEFAULT 'clear',  -- auto_flag | needs_review | clear
    status       TEXT    NOT NULL DEFAULT 'ok',     -- ok | api_error | diff_unavailable | unclassified
                                             -- unclassified: reached the model but produced no
                                             -- verdict (model down / unparseable reply / abstained);
                                             -- routed to needs_review, never left looking benign
    model_version TEXT,                      -- which model/prompt produced this verdict; lets a
                                             -- replay tell one model's verdicts from another's
    reasoning    TEXT,                       -- short model rationale

    event_time   TIMESTAMPTZ,               -- meta.dt from the edit (event time)
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS edits_label_idx        ON edits (label);
CREATE INDEX IF NOT EXISTS edits_event_time_idx   ON edits (event_time DESC);
CREATE INDEX IF NOT EXISTS edits_review_state_idx ON edits (review_state);
-- Partial index for the deception feed: small, hot, queried on every dashboard refresh.
CREATE INDEX IF NOT EXISTS edits_mismatch_idx     ON edits (event_time DESC) WHERE comment_diff_mismatch;
-- Partial index for the (hopefully rare) label-anomaly diagnostics feed.
CREATE INDEX IF NOT EXISTS edits_raw_label_idx    ON edits (event_time DESC) WHERE raw_label <> '';

-- Label taxonomy (documented, not enforced):
--   vandalism         malicious / damaging change
--   spam              promotional / link spam
--   substantive_edit  meaningful content addition or change
--   minor_edit        typo, copyedit, formatting, wikignome work
--   revert            undo / rollback of a previous edit
--   other             legitimate but none of the above
--   unknown           seeded default; also the HARD-STOP label when the diff could
--                     not be fetched (we never classify an edit blind)

-- Analytics: label distribution (the "actionable output").
CREATE OR REPLACE VIEW edit_label_summary AS
SELECT label,
       count(*)                              AS edits,
       round(avg(confidence)::numeric, 2)    AS avg_confidence,
       sum(enriched::int)                    AS classified_count,
       sum((rule_hit <> '')::int)            AS rule_caught
FROM edits
GROUP BY label
ORDER BY edits DESC;

-- ── Two-band moderation queues (ClueBot NG pattern: separate "confident enough to
--    act" from "suspicious enough for a human to look"). Collapsing both into one
--    feed either floods the queue with false positives or buries borderline vandalism.

-- ACTION band: high-confidence / rule-caught — what a bot or a fast-acting moderator
-- would treat as actionable now.
CREATE OR REPLACE VIEW recent_action AS
SELECT event_time, wiki, title, user_name, is_anon, label, confidence,
       rule_hit, byte_delta, comment, reasoning,
       -- clickable link to the RENDERED diff on the source wiki (diff=prev shows the
       -- change that produced rev_new). Cheaper than storing the diff, and always current.
       'https://' || server_name || '/w/index.php?diff=prev&oldid=' || rev_new AS diff_url
FROM edits
WHERE review_state = 'auto_flag'
ORDER BY event_time DESC
LIMIT 100;

-- REVIEW band: the human triage queue. Deliberate-deception edits (comment lies about
-- the diff) sort to the TOP regardless of confidence — that mismatch is the tell.
CREATE OR REPLACE VIEW recent_review AS
SELECT event_time, wiki, title, user_name, is_anon, label, confidence,
       comment_diff_mismatch, status, byte_delta, comment, reasoning,
       'https://' || server_name || '/w/index.php?diff=prev&oldid=' || rev_new AS diff_url
FROM edits
WHERE review_state = 'needs_review'
ORDER BY comment_diff_mismatch DESC, event_time DESC
LIMIT 100;

-- Back-compat: the original single high-signal feed (kept so existing Grafana panels
-- keep working; new panels should prefer the two bands above).
CREATE OR REPLACE VIEW recent_flagged AS
SELECT event_time, wiki, title, user_name, label, confidence, byte_delta, comment, reasoning,
       'https://' || server_name || '/w/index.php?diff=prev&oldid=' || rev_new AS diff_url
FROM edits
WHERE label IN ('vandalism', 'spam')
ORDER BY event_time DESC
LIMIT 100;

-- Diagnostics: label ANOMALIES — rows where the model emitted a label outside our
-- taxonomy. `label` was coerced to 'unknown'; `raw_label` is what the model actually
-- said. An empty feed is the healthy state; entries here mean the prompt/model is
-- drifting and needs attention. This is a MODEL-QA feed, distinct from moderation.
CREATE OR REPLACE VIEW label_anomalies AS
SELECT event_time, wiki, title, raw_label, label, confidence, model_version, reasoning,
       'https://' || server_name || '/w/index.php?diff=prev&oldid=' || rev_new AS diff_url
FROM edits
WHERE raw_label <> ''
ORDER BY event_time DESC
LIMIT 100;
