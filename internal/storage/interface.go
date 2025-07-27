package storage

import (
	"context"
	"io"
	"time"
)

type ObjectMetadata struct {
	Size    int64
	ModTime time.Time
}

type UploadedPart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

type ObjectStorage interface {
	SaveObject(ctx context.Context, bucket, key string, r io.Reader) error
	GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error)
	DeleteObject(ctx context.Context, bucket, key string) error
	StatObject(ctx context.Context, bucket string, key string) (ObjectMetadata, error)
}

type BucketStorage interface {
	CreateBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
}

type MultipartStorage interface {
	InitMultipartUpload(ctx context.Context, bucket, key string) (string, error)
	UploadPart(ctx context.Context, uploadID string, partNumber int, data io.Reader) (string, error)
	CompleteUpload(ctx context.Context, uploadID string, parts []UploadedPart) (string, error)
	AbortUpload(ctx context.Context, uploadID string) error
}

type StorageEngine interface {
	ObjectStorage
	MultipartStorage
	BucketStorage
}
