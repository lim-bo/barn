-- +goose Up
ALTER TABLE users
ADD COLUMN username TEXT UNIQUE,
ADD COLUMN password_hash TEXT;