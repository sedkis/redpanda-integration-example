-- Wikipedia edit-stream classifier — Postgres schema
-- Loaded automatically by the postgres container on first boot
-- (mounted into /docker-entrypoint-initdb.d).

-- One row per Wikipedia revision. The NEW revision id is the natural,
-- idempotent UPSERT key: re-processing the same edit (retry, or the
-- confidence-gated second pass) updates the row instead of duplicating it.
CREATE TABLE IF NOT EXISTS edits (
    rev_new      BIGINT PRIMARY KEY,        -- revision.new  (upsert key)
    rev_old      BIGINT,                     -- revision.old
    wiki         TEXT,                       -- e.g. enwiki
    server_name  TEXT,                       -- e.g. en.wikipedia.org
    title        TEXT,
    user_name    TEXT,
    is_bot       BOOLEAN,
    edit_type    TEXT,                       -- edit | new | ...
    comment      TEXT,
    byte_delta   INTEGER,                    -- length.new - length.old

    -- Model output. `label` is intentionally TEXT with no CHECK constraint:
    -- the pipeline already normalizes to the enum, and we would rather a
    -- surprising label land as a ROW than have a constraint silently drop it.
    label        TEXT    NOT NULL DEFAULT 'unknown',
    confidence   REAL    NOT NULL DEFAULT 0.0,
    enriched     BOOLEAN NOT NULL DEFAULT FALSE,  -- did the diff second-pass run?
    reasoning    TEXT,                            -- short model rationale

    event_time   TIMESTAMPTZ,               -- meta.dt from the edit (event time)
    ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS edits_label_idx      ON edits (label);
CREATE INDEX IF NOT EXISTS edits_event_time_idx ON edits (event_time DESC);

-- Label taxonomy (documented, not enforced):
--   vandalism         malicious / damaging change
--   spam              promotional / link spam
--   substantive_edit  meaningful content addition or change
--   minor_edit        typo, copyedit, formatting, wikignome work
--   revert            undo / rollback of a previous edit
--   other             legitimate but none of the above
--   unknown           seeded default before/if the model fails

-- Analytics: label distribution (the "actionable output").
CREATE OR REPLACE VIEW edit_label_summary AS
SELECT label,
       count(*)                              AS edits,
       round(avg(confidence)::numeric, 2)    AS avg_confidence,
       sum(enriched::int)                    AS second_pass_count
FROM edits
GROUP BY label
ORDER BY edits DESC;

-- Analytics: the high-signal feed a moderator would actually watch.
CREATE OR REPLACE VIEW recent_flagged AS
SELECT event_time, wiki, title, user_name, label, confidence, byte_delta, comment
FROM edits
WHERE label IN ('vandalism', 'spam')
ORDER BY event_time DESC
LIMIT 100;
