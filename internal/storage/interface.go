package storage

import (
	"context"
	"io"
	"time"

	"github.com/google/uuid"
)

type ObjectMetadata struct {
	Name    string
	Size    int64
	ModTime time.Time
}

type UploadedPart struct {
	PartNumber int
	ETag       string
}

type UploadMetadata struct {
	ID     uuid.UUID
	Bucket string
	Key    string
	Parts  []UploadedPart
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
	InitMultipartUpload(ctx context.Context) (uuid.UUID, error)
	UploadPart(ctx context.Context, uploadID uuid.UUID, partNumber int, data io.Reader) (string, error)
	CompleteUpload(ctx context.Context, upload UploadMetadata) (string, error)
	AbortUpload(ctx context.Context, uploadID uuid.UUID) error
}

type StorageEngine interface {
	ObjectStorage
	MultipartStorage
	BucketStorage
}
