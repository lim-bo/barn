-- +goose Up
ALTER TABLE objects RENAME COLUMN created_at TO last_modified;