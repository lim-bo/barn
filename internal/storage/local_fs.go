package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
)

// Local filesystem controller for actions with files.
// File path example: {root}/{bucket_name}/{key}
// (NB: In this project bucket_name is {owner_id}_{bucket_id})
type LocalFS struct {
	basePath string
}

// root is files' data dir
func NewLocalFS(root string) *LocalFS {
	return &LocalFS{
		basePath: root,
	}
}

// Saves new (or truncates if exists) file in given bucket and key
func (lfs *LocalFS) SaveObject(ctx context.Context, bucket, key string, r io.Reader) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Join(lfs.basePath, bucket, key)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, r)
	return err
}

// Searchs for file in bucket by key, returns io.ReadCloser for its content. If there is no such
// file, returns PathError
func (lfs *LocalFS) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path := filepath.Join(lfs.basePath, bucket, key)
	return os.Open(path)
}

// Deletes file with key in bucket
func (lfs *LocalFS) DeleteObject(ctx context.Context, bucket, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Join(lfs.basePath, bucket, key)
	return os.Remove(path)
}

// Returns Name, Size and ModTime of file with key in bucket
func (lfs *LocalFS) StatObject(ctx context.Context, bucket string, key string) (ObjectMetadata, error) {
	if err := ctx.Err(); err != nil {
		return ObjectMetadata{}, err
	}
	path := filepath.Join(lfs.basePath, bucket, key)
	stat, err := os.Stat(path)
	if err != nil {
		return ObjectMetadata{}, err
	}
	return ObjectMetadata{
		Name:    stat.Name(),
		Size:    stat.Size(),
		ModTime: stat.ModTime(),
	}, nil
}

// Local filesystem repository fot buckets operations
type BucketLocalFS struct {
	basePath string
}

// root is buckets' upper directory
func NewBucketLocalFS(root string) *BucketLocalFS {
	return &BucketLocalFS{
		basePath: root,
	}
}

// Creates bucket (dir) with given name, if it already exists does nothing and returns nil
func (lfs *BucketLocalFS) CreateBucket(ctx context.Context, bucket string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Join(lfs.basePath, bucket)
	return os.MkdirAll(path, 0755)
}

// Deletes bucket. If bucket dir doesn't exists, does nothing
func (lfs *BucketLocalFS) DeleteBucket(ctx context.Context, bucket string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path := filepath.Join(lfs.basePath, bucket)
	return os.RemoveAll(path)
}
