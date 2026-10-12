-- +goose Up
-- Stored Kafka positions (ADR 0008 and its addendum, option A): one row per
-- (consumer group, topic, partition), written in the same transaction as the
-- aggregate upsert of a batch. next_offset is the next offset to read, i.e. the
-- offset of the last applied record plus one. The table holds one row per
-- partition (6 today); the primary key is the only access path.
CREATE TABLE consumer_offsets (
    consumer_group TEXT NOT NULL,
    topic          TEXT NOT NULL,
    partition      INT NOT NULL,
    next_offset    BIGINT NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (consumer_group, topic, partition),
    CHECK (partition >= 0),
    CHECK (next_offset >= 0)
);

-- Runtime grant (ADR 0010): read on assignment, insert the first position,
-- compare-and-set it on every flush. No DELETE or TRUNCATE: resetting positions
-- is an operator task run as the owner (ADR 0008, "Operations").
GRANT SELECT, INSERT, UPDATE ON consumer_offsets TO analytics_app;

-- +goose Down
DROP TABLE consumer_offsets;
