package repos_test

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lim-bo/barn/internal/errvalues"
	repos "github.com/lim-bo/barn/internal/repository"
	"github.com/lim-bo/barn/pkg/models"
	"github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"
)

func TestSaveObject(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	key := "file.txt"
	bucket := "test_bucket"
	bucketID := uuid.New()
	objRepo := repos.NewObjectsRepoWithConn(conn)
	obj := models.Object{
		Key:  key,
		Size: uint64(len(key)),
		Etag: "\"testetag\"",
	}
	bucketIDQuery := regexp.QuoteMeta(`SELECT id from buckets WHERE name = $1;`)
	selectQuery := regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id = $1 AND key = $2);`)
	insertQuery := regexp.QuoteMeta(`INSERT INTO objects (bucket_id, key, size, etag) VALUES ($1, $2, $3, $4);`)
	updateQuery := regexp.QuoteMeta(`UPDATE objects SET size = $1, etag = $2, last_modified = now() WHERE bucket_id = $3 AND key = $4;`)
	t.Run("saved new", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(bucketIDQuery).WithArgs(bucket).WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(bucketID))
		conn.ExpectQuery(selectQuery).WithArgs(bucketID, key).WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))
		conn.ExpectExec(insertQuery).WithArgs(bucketID, obj.Key, obj.Size, obj.Etag).WillReturnResult(pgxmock.NewResult("INSERT", 1))
		conn.ExpectCommit()
		err := objRepo.SaveObject(bucket, &obj)
		assert.NoError(t, err)
	})
	t.Run("updated existed", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(bucketIDQuery).WithArgs(bucket).WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(bucketID))
		conn.ExpectQuery(selectQuery).WithArgs(bucketID, key).WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
		conn.ExpectExec(updateQuery).WithArgs(obj.Size, obj.Etag, bucketID, key).WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		conn.ExpectCommit()
		err := objRepo.SaveObject(bucket, &obj)
		assert.NoError(t, err)
	})

}

func TestGetObjectInfo(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	objRepo := repos.NewObjectsRepoWithConn(conn)
	query := regexp.QuoteMeta(`SELECT o.id, o.bucket_id, o.size, o.etag, o.last_modified FROM objects o INNER JOIN buckets b ON
o.bucket_id = b.id WHERE b.name = $1 AND o.key = $2;`)
	bucket := "test_bucket"
	key := "file.txt"
	obj := models.Object{
		ID:           uuid.New(),
		BucketID:     uuid.New(),
		Key:          key,
		Size:         16,
		Etag:         "\"example_etag\"",
		LastModified: time.Now(),
	}
	t.Run("successful", func(t *testing.T) {
		conn.ExpectQuery(query).
			WithArgs(bucket, key).
			WillReturnRows(pgxmock.NewRows([]string{"id", "bucket_id", "size", "etag", "last_modified"}).AddRow(obj.ID, obj.BucketID, obj.Size, obj.Etag, obj.LastModified))

		info, err := objRepo.GetObjectInfo(bucket, key)
		assert.NoError(t, err)
		assert.Equal(t, obj, *info)
	})
	t.Run("not found", func(t *testing.T) {
		conn.ExpectQuery(query).
			WithArgs(bucket, key).
			WillReturnError(pgx.ErrNoRows)
		_, err := objRepo.GetObjectInfo(bucket, key)
		assert.ErrorIs(t, err, errvalues.ErrUnexistObject)
	})
	t.Run("db error", func(t *testing.T) {
		conn.ExpectQuery(query).
			WithArgs(bucket, key).
			WillReturnError(errors.New("db error"))
		_, err := objRepo.GetObjectInfo(bucket, key)
		assert.Error(t, err)
	})
}

func TestDeleteObject(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	objRepo := repos.NewObjectsRepoWithConn(conn)
	query := regexp.QuoteMeta(`DELETE FROM objects USING buckets WHERE objects.bucket_id = buckets.id AND buckets.name = $1 AND objects.key = $2;`)
	bucket, key := "test_bucket", "file.txt"
	t.Run("successful", func(t *testing.T) {
		conn.ExpectExec(query).WithArgs(bucket, key).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		err := objRepo.DeleteObject(bucket, key)
		assert.NoError(t, err)
	})
	t.Run("not found", func(t *testing.T) {
		conn.ExpectExec(query).WithArgs(bucket, key).WillReturnResult(pgxmock.NewResult("DELETE", 0))
		err := objRepo.DeleteObject(bucket, key)
		assert.ErrorIs(t, err, errvalues.ErrUnexistObject)
	})
	t.Run("db error", func(t *testing.T) {
		conn.ExpectExec(query).WithArgs(bucket, key).WillReturnError(errors.New("db error"))
		err := objRepo.DeleteObject(bucket, key)
		assert.Error(t, err)
	})
}

func TestObjectsIntegrational(t *testing.T) {
	cfg := setupTestDB(t)
	bucket := "test_bucket"
	obj := &models.Object{
		Key:  "file.txt",
		Size: 16,
		Etag: "\"example_etag\"",
	}
	// Need to add new bucket
	br := repos.NewBucketRepo(cfg)
	_, err := br.CreateBucket(ownerID, "test_bucket")
	if err != nil {
		t.Fatal(err)
	}

	objRepo := repos.NewObjectsRepo(cfg)
	t.Run("Saving new object", func(t *testing.T) {
		err := objRepo.SaveObject(bucket, obj)
		assert.NoError(t, err)
	})
	t.Run("Getting object info", func(t *testing.T) {
		obj, err = objRepo.GetObjectInfo(bucket, obj.Key)
		assert.NoError(t, err)
		t.Log("obj info: ", obj)
	})
	obj.Size = 128
	t.Run("Updating existed object", func(t *testing.T) {
		err := objRepo.SaveObject(bucket, obj)
		assert.NoError(t, err)
	})
	t.Run("Check for new obj info", func(t *testing.T) {
		result, err := objRepo.GetObjectInfo(bucket, obj.Key)
		assert.NoError(t, err)
		assert.True(t, func() bool {
			return obj.Key == result.Key &&
				obj.BucketID == result.BucketID &&
				obj.Etag == result.Etag &&
				obj.Size == result.Size &&
				obj.ID == result.ID
		}())
	})
	t.Run("Deletion object", func(t *testing.T) {
		err := objRepo.DeleteObject(bucket, obj.Key)
		assert.NoError(t, err)
	})
	t.Run("Check if obj doesn't exist", func(t *testing.T) {
		_, err := objRepo.GetObjectInfo(bucket, obj.Key)
		assert.ErrorIs(t, err, errvalues.ErrUnexistObject)
	})
}
