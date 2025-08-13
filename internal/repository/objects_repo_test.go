package repos_test

import (
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/lim-bo/barn/internal/errvalues"
	repos "github.com/lim-bo/barn/internal/repository"
	"github.com/lim-bo/barn/internal/services"
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
	bucketIDQuery := regexp.QuoteMeta(`SELECT id from buckets WHERE name = $1 AND owner_id = $2;`)
	selectQuery := regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id = $1 AND key = $2);`)
	insertQuery := regexp.QuoteMeta(`INSERT INTO objects (bucket_id, key, size, etag) VALUES ($1, $2, $3, $4);`)
	updateQuery := regexp.QuoteMeta(`UPDATE objects SET size = $1, etag = $2, last_modified = now() WHERE bucket_id = $3 AND key = $4;`)
	t.Run("saved new", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(bucketIDQuery).WithArgs(bucket, ownerID).WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(bucketID))
		conn.ExpectQuery(selectQuery).WithArgs(bucketID, key).WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(false))
		conn.ExpectExec(insertQuery).WithArgs(bucketID, obj.Key, obj.Size, obj.Etag).WillReturnResult(pgxmock.NewResult("INSERT", 1))
		conn.ExpectCommit()
		err := objRepo.SaveObject(ownerID, bucket, &obj)
		assert.NoError(t, err)
	})
	t.Run("updated existed", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(bucketIDQuery).WithArgs(bucket, ownerID).WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(bucketID))
		conn.ExpectQuery(selectQuery).WithArgs(bucketID, key).WillReturnRows(pgxmock.NewRows([]string{"exists"}).AddRow(true))
		conn.ExpectExec(updateQuery).WithArgs(obj.Size, obj.Etag, bucketID, key).WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		conn.ExpectCommit()
		err := objRepo.SaveObject(ownerID, bucket, &obj)
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
o.bucket_id = b.id WHERE b.name = $1 AND o.key = $2 AND b.owner_id = $3;`)
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
			WithArgs(bucket, key, ownerID).
			WillReturnRows(pgxmock.NewRows([]string{"id", "bucket_id", "size", "etag", "last_modified"}).AddRow(obj.ID, obj.BucketID, obj.Size, obj.Etag, obj.LastModified))

		info, err := objRepo.GetObjectInfo(ownerID, bucket, key)
		assert.NoError(t, err)
		assert.Equal(t, obj, *info)
	})
	t.Run("not found", func(t *testing.T) {
		conn.ExpectQuery(query).
			WithArgs(bucket, key, ownerID).
			WillReturnError(pgx.ErrNoRows)
		_, err := objRepo.GetObjectInfo(ownerID, bucket, key)
		assert.ErrorIs(t, err, errvalues.ErrUnexistObject)
	})
	t.Run("db error", func(t *testing.T) {
		conn.ExpectQuery(query).
			WithArgs(bucket, key, ownerID).
			WillReturnError(errors.New("db error"))
		_, err := objRepo.GetObjectInfo(ownerID, bucket, key)
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
	query := regexp.QuoteMeta(`DELETE FROM objects USING buckets WHERE objects.bucket_id = buckets.id AND buckets.name = $1 AND objects.key = $2 AND buckets.owner_id = $3;`)
	bucket, key := "test_bucket", "file.txt"
	t.Run("successful", func(t *testing.T) {
		conn.ExpectExec(query).WithArgs(bucket, key, ownerID).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		err := objRepo.DeleteObject(ownerID, bucket, key)
		assert.NoError(t, err)
	})
	t.Run("not found", func(t *testing.T) {
		conn.ExpectExec(query).WithArgs(bucket, key, ownerID).WillReturnResult(pgxmock.NewResult("DELETE", 0))
		err := objRepo.DeleteObject(ownerID, bucket, key)
		assert.ErrorIs(t, err, errvalues.ErrUnexistObject)
	})
	t.Run("db error", func(t *testing.T) {
		conn.ExpectExec(query).WithArgs(bucket, key, ownerID).WillReturnError(errors.New("db error"))
		err := objRepo.DeleteObject(ownerID, bucket, key)
		assert.Error(t, err)
	})
}

func TestListingObjects(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	objRepo := repos.NewObjectsRepoWithConn(conn)
	query := regexp.QuoteMeta(`SELECT key, size, etag, last_modified FROM objects o INNER JOIN buckets b ON
o.bucket_id = b.id WHERE b.owner_id = $1 AND b.name = $2 OFFSET $3 LIMIT $4;`)
	bucket := "test_bucket"
	rows := pgxmock.NewRows([]string{"key", "size", "etag", "last_modified"})
	ts := time.Now()
	for i := range 10 {
		key := fmt.Sprintf("key_n_%d", i)
		rows.AddRow(key, uint64(i), fmt.Sprintf("\"etag_n_%d\"", i), ts.Add(time.Second*time.Duration(i)))
	}
	t.Run("successfully listed all", func(t *testing.T) {
		conn.ExpectQuery(query).WithArgs(ownerID, bucket, 0, 10).WillReturnRows(rows)
		result, err := objRepo.ListObjects(ownerID, bucket, &services.PaginationOpts{
			Offset: 0,
			Limit:  10,
		})
		assert.NoError(t, err)
		for i, o := range result {
			assert.True(t, func() bool {
				return o.Key == fmt.Sprintf("key_n_%d", i) &&
					o.Etag == fmt.Sprintf("\"etag_n_%d\"", i) &&
					o.LastModified.Equal(ts.Add(time.Second*time.Duration(i))) &&
					o.Size == uint64(i)
			}())
		}
	})
	// Offset + limit test is useless because of mock
	t.Run("empty result", func(t *testing.T) {
		conn.ExpectQuery(query).WithArgs(ownerID, bucket, 0, 10).WillReturnRows(pgxmock.NewRows([]string{"key", "size", "etag", "last_modified"}))
		result, err := objRepo.ListObjects(ownerID, bucket, &services.PaginationOpts{
			Offset: 0,
			Limit:  10,
		})
		assert.NoError(t, err)
		assert.True(t, len(result) == 0)
	})
	t.Run("db error", func(t *testing.T) {
		conn.ExpectQuery(query).WithArgs(ownerID, bucket, 0, 10).WillReturnError(errors.New("db error"))
		_, err := objRepo.ListObjects(ownerID, bucket, &services.PaginationOpts{
			Offset: 0,
			Limit:  10,
		})
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
		err := objRepo.SaveObject(ownerID, bucket, obj)
		assert.NoError(t, err)
	})
	t.Run("Getting object info", func(t *testing.T) {
		obj, err = objRepo.GetObjectInfo(ownerID, bucket, obj.Key)
		assert.NoError(t, err)
		t.Log("obj info: ", obj)
	})
	obj.Size = 128
	t.Run("Updating existed object", func(t *testing.T) {
		err := objRepo.SaveObject(ownerID, bucket, obj)
		assert.NoError(t, err)
	})
	t.Run("Check for new obj info", func(t *testing.T) {
		result, err := objRepo.GetObjectInfo(ownerID, bucket, obj.Key)
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
		err := objRepo.DeleteObject(ownerID, bucket, obj.Key)
		assert.NoError(t, err)
	})
	t.Run("Check if obj doesn't exist", func(t *testing.T) {
		_, err := objRepo.GetObjectInfo(ownerID, bucket, obj.Key)
		assert.ErrorIs(t, err, errvalues.ErrUnexistObject)
	})

	t.Run("Adding few elements", func(t *testing.T) {
		for i := range 10 {
			obj := models.Object{
				Key:  fmt.Sprintf("key_n_%d", i),
				Size: uint64(i),
				Etag: fmt.Sprintf("\"etag_n_%d\"", i),
			}
			err = objRepo.SaveObject(ownerID, bucket, &obj)
			assert.NoError(t, err)
		}
	})

	t.Run("listing all", func(t *testing.T) {
		result, err := objRepo.ListObjects(ownerID, bucket, &services.PaginationOpts{
			Offset: 0,
			Limit:  10,
		})
		assert.NoError(t, err)
		for i, o := range result {
			assert.Equal(t, fmt.Sprintf("key_n_%d", i), o.Key)
		}
	})
	t.Run("listing with offset and limit", func(t *testing.T) {
		result, err := objRepo.ListObjects(ownerID, bucket, &services.PaginationOpts{
			Offset: 3,
			Limit:  5,
		})
		assert.NoError(t, err)
		assert.True(t, len(result) == 5 && result[0].Key == fmt.Sprintf("key_n_%d", 3))
	})
}
