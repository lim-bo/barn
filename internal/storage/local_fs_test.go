package storage_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"

	"github.com/lim-bo/barn/internal/storage"
	"github.com/stretchr/testify/assert"
)

func TestLocalFS(t *testing.T) {
	err := os.Mkdir("../../data/test_bucket", os.ModeDir)
	if err != nil {
		t.Fatal(err)
	}
	lfs := storage.NewLocalFS("../../data")
	content := []byte("hello world")
	data := bytes.NewReader(content)
	bucket := "test_bucket"
	key := "message.txt"
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
	os.Remove("../../data/" + bucket)
}
