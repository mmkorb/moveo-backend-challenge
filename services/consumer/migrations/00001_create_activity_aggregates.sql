-- +goose Up
-- The aggregate table (ADR 0009), verbatim from the challenge summary. The
-- consumer's stored positions live in consumer_offsets (migration 00002).
CREATE TABLE activity_aggregates (
    activity_date   DATE NOT NULL,
    user_id         TEXT NOT NULL,
    activity_count  BIGINT NOT NULL DEFAULT 0,
    page_view_count BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (activity_date, user_id),
    CHECK (activity_count >= 0),
    CHECK (page_view_count >= 0),
    CHECK (page_view_count <= activity_count)
);

CREATE INDEX idx_activity_aggregates_user
    ON activity_aggregates (user_id);

-- Runtime grant (ADR 0010). SELECT is needed because ON CONFLICT DO UPDATE reads
-- the existing row. No DELETE, TRUNCATE or DDL. The role is created by
-- deploy/postgres/init.sql.
GRANT SELECT, INSERT, UPDATE ON activity_aggregates TO analytics_app;

-- +goose Down
DROP TABLE activity_aggregates;
