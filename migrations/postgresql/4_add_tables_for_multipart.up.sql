CREATE TABLE multipart_uploads (
    upload_id   UUID PRIMARY KEY,
    bucket_id   UUID NOT NULL REFERENCES buckets(id) ON DELETE CASCADE,
    key         TEXT NOT NULL,
    status      TEXT NOT NULL, -- inited, aborted or completed
    created_at  TIMESTAMP DEFAULT now()
);

CREATE TABLE multipart_parts (
    upload_id   UUID REFERENCES multipart_uploads(upload_id) ON DELETE CASCADE,
    part_number INT NOT NULL,
    etag        TEXT NOT NULL,
    size        BIGINT NOT NULL,
    created_at  TIMESTAMP DEFAULT now(),
    PRIMARY KEY (upload_id, part_number)
);