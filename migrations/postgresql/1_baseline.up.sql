-- +goose Up
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS users (
    id               UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    access_key       TEXT        NOT NULL UNIQUE,
    secret_key_hash  TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'active',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS buckets (
    id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    name         TEXT        NOT NULL,
    owner_id     UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    region       TEXT        NOT NULL DEFAULT 'us-east-1',
    acl          JSONB       DEFAULT '{}'::JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (owner_id, name)
);

CREATE INDEX IF NOT EXISTS idx_buckets_owner_id
    ON buckets (owner_id);

CREATE TABLE IF NOT EXISTS objects (
    id           UUID        PRIMARY KEY DEFAULT uuid_generate_v4(),
    bucket_id    UUID        NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    key          TEXT        NOT NULL,
    size         BIGINT      NOT NULL,
    etag         TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (bucket_id, key)
);

CREATE INDEX IF NOT EXISTS idx_objects_bucket_id
    ON objects (bucket_id);