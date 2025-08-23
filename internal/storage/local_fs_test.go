package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/google/uuid"
	"github.com/lim-bo/barn/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestLocalFS(t *testing.T) {
	lfs := storage.NewLocalFS("../../data")
	content := []byte("hello world")
	data := bytes.NewReader(content)
	bucket := "test_bucket"
	key := "message.txt"
	bucketLfs := storage.NewBucketLocalFS("../../data")
	assert.NoError(t, bucketLfs.CreateBucket(context.Background(), bucket))
	t.Run("object saved", func(t *testing.T) {
		err := lfs.SaveObject(context.Background(), bucket, key, data)
		assert.NoError(t, err)
	})
	t.Run("error on no bucket", func(t *testing.T) {
		err := lfs.SaveObject(context.Background(), "unexist_bucket", key, data)
		assert.Error(t, err)
	})
	t.Run("got object", func(t *testing.T) {
		file, err := lfs.GetObject(context.Background(), bucket, key)
		assert.NoError(t, err)
		defer file.Close()
		result, err := io.ReadAll(file)
		assert.NoError(t, err)
		assert.EqualValues(t, content, result)
	})
	t.Run("no file", func(t *testing.T) {
		_, err := lfs.GetObject(context.Background(), bucket, "unexist_key.txt")
		assert.Error(t, err)
	})
	t.Run("got stat", func(t *testing.T) {
		stat, err := lfs.StatObject(context.Background(), bucket, key)
		assert.NoError(t, err)
		t.Logf("file stat: %v", stat)
	})
	t.Run("no file for stat", func(t *testing.T) {
		_, err := lfs.StatObject(context.Background(), bucket, "unexist_key.txt")
		assert.Error(t, err)
	})
	t.Run("deleted", func(t *testing.T) {
		err := lfs.DeleteObject(context.Background(), bucket, key)
		assert.NoError(t, err)
	})
	t.Run("no file for deletion", func(t *testing.T) {
		err := lfs.DeleteObject(context.Background(), bucket, key)
		assert.Error(t, err)
	})
	assert.NoError(t, bucketLfs.DeleteBucket(context.Background(), bucket))
}

func TestMultipartLocalFS(t *testing.T) {
	lfs := storage.NewMultipartLocalFS("../../data")
	bucket := "test_bucket"
	key := "biiiig_file.txt"
	ctx := context.Background()

	fileParts := make([][]byte, 0, 5)
	for i := range 5 {
		content := fmt.Sprintf("row number %d\n", i)
		fileParts = append(fileParts, []byte(content))
	}
	parts := make([]storage.UploadedPart, 0, 5)
	var uploadID uuid.UUID
	var err error
	t.Run("init upload", func(t *testing.T) {
		uploadID, err = lfs.InitMultipartUpload(ctx)
		assert.NoError(t, err)
	})
	t.Run("upload parts", func(t *testing.T) {
		for i, data := range fileParts {
			src := bytes.NewReader(data)
			etag, err := lfs.UploadPart(ctx, uploadID, i, src)
			assert.NoError(t, err)
			parts = append(parts, storage.UploadedPart{
				ETag:       etag,
				PartNumber: i,
			})
		}
	})
	t.Run("complete upload", func(t *testing.T) {
		etag, _, err := lfs.CompleteUpload(ctx, storage.UploadMetadata{
			Bucket: bucket,
			Key:    key,
			ID:     uploadID,
			Parts:  parts,
		})
		assert.NoError(t, err)
		fmt.Println("result etag: " + etag)
	})
	t.Run("init and abort upload", func(t *testing.T) {
		newUploadID, err := lfs.InitMultipartUpload(ctx)
		assert.NoError(t, err)
		err = lfs.AbortUpload(ctx, newUploadID)
		assert.NoError(t, err)
	})
}
