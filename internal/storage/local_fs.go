package storage

import (
	"context"
	"io"
	"os"
	"path/filepath"
)

type LocalFS struct {
	basePath string
}

func NewLocalFS(root string) *LocalFS {
	return &LocalFS{
		basePath: root,
	}
}

func (lfs *LocalFS) SaveObject(ctx context.Context, bucket, key string, r io.Reader) error {
	path := filepath.Join(lfs.basePath, bucket, key)
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = io.Copy(file, r)
	return err
}

func (lfs *LocalFS) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	path := filepath.Join(lfs.basePath, bucket, key)
	return os.Open(path)
}

func (lfs *LocalFS) DeleteObject(ctx context.Context, bucket, key string) error {
	path := filepath.Join(lfs.basePath, bucket, key)
	return os.Remove(path)
}

func (lfs *LocalFS) StatObject(ctx context.Context, bucket string, key string) (ObjectMetadata, error) {
	path := filepath.Join(lfs.basePath, bucket, key)
	stat, err := os.Stat(path)
	if err != nil {
		return ObjectMetadata{}, err
	}
	return ObjectMetadata{
		Size:    stat.Size(),
		ModTime: stat.ModTime(),
	}, nil
}
