package repos_test

import (
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lim-bo/barn/internal/errvalues"
	repos "github.com/lim-bo/barn/internal/repository"
	"github.com/lim-bo/barn/internal/services"
	"github.com/lim-bo/barn/pkg/models"
	"github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"
)

func TestInitUpload(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	repo := repos.NewMultipartRepoWithConn(conn)
	upload := models.Upload{
		ID:      uuid.New(),
		Bucket:  "test_bucket",
		Key:     "file.txt",
		OwnerID: uuid.New(),
	}
	bucketID := uuid.New()
	selectQuery := regexp.QuoteMeta(`SELECT id FROM buckets WHERE name = $1 AND owner_id = $2;`)
	insertQuery := regexp.QuoteMeta(`INSERT INTO multipart_uploads (upload_id, bucket_id, key, status) VALUES ($1, $2, $3, $4);`)
	t.Run("successfull", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(selectQuery).
			WithArgs(upload.Bucket, upload.OwnerID).
			WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(bucketID))
		conn.ExpectExec(insertQuery).
			WithArgs(upload.ID, bucketID, upload.Key, services.MultipartStatusInited).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
		conn.ExpectCommit()
		err = repo.CreateUpload(&upload)
		assert.NoError(t, err)
	})
	t.Run("select error: no bucket", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(selectQuery).
			WithArgs(upload.Bucket, upload.OwnerID).
			WillReturnError(pgx.ErrNoRows)
		conn.ExpectRollback()
		err = repo.CreateUpload(&upload)
		assert.ErrorIs(t, err, errvalues.ErrNoBucket)
	})
	t.Run("inserting error", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(selectQuery).
			WithArgs(upload.Bucket, upload.OwnerID).
			WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(bucketID))
		conn.ExpectExec(insertQuery).
			WithArgs(upload.ID, bucketID, upload.Key, services.MultipartStatusInited).
			WillReturnError(errors.New("error"))
		conn.ExpectRollback()
		err = repo.CreateUpload(&upload)
		assert.Error(t, err)
	})
}

func TestChangeState(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	repo := repos.NewMultipartRepoWithConn(conn)
	uploadID := uuid.New()
	deleteQuery := regexp.QuoteMeta(`DELETE FROM multipart_parts WHERE upload_id = $1;`)
	updateQuery := regexp.QuoteMeta(`UPDATE multipart_uploads SET status = $1 WHERE upload_id = $2;`)
	t.Run("-> completed: successful", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(regexp.QuoteMeta(`SELECT status FROM multipart_uploads WHERE upload_id = $1;`)).
			WithArgs(uploadID).
			WillReturnRows(pgxmock.NewRows([]string{"status"}).AddRow("inited"))
		conn.ExpectExec(deleteQuery).
			WithArgs(uploadID).
			WillReturnResult(pgxmock.NewResult("DELETE", 10))
		conn.ExpectExec(updateQuery).
			WithArgs(services.MultipartStatusCompleted, uploadID).
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		conn.ExpectCommit()
		err = repo.ChangeUploadState(uploadID, services.MultipartStatusCompleted)
		assert.NoError(t, err)
	})
	t.Run("-> aborted: successful", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(regexp.QuoteMeta(`SELECT status FROM multipart_uploads WHERE upload_id = $1;`)).
			WithArgs(uploadID).
			WillReturnRows(pgxmock.NewRows([]string{"status"}).AddRow("inited"))
		conn.ExpectExec(deleteQuery).
			WithArgs(uploadID).
			WillReturnResult(pgxmock.NewResult("DELETE", 10))
		conn.ExpectExec(updateQuery).
			WithArgs(services.MultipartStatusAborted, uploadID).
			WillReturnResult(pgxmock.NewResult("UPDATE", 1))
		conn.ExpectCommit()
		err = repo.ChangeUploadState(uploadID, services.MultipartStatusAborted)
		assert.NoError(t, err)
	})
	t.Run("delete error", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(regexp.QuoteMeta(`SELECT status FROM multipart_uploads WHERE upload_id = $1;`)).
			WithArgs(uploadID).
			WillReturnRows(pgxmock.NewRows([]string{"status"}).AddRow("inited"))
		conn.ExpectExec(deleteQuery).
			WithArgs(uploadID).
			WillReturnError(errors.New("db error"))
		conn.ExpectRollback()
		err = repo.ChangeUploadState(uploadID, services.MultipartStatusAborted)
		assert.Error(t, err)
	})
	t.Run("update error", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(regexp.QuoteMeta(`SELECT status FROM multipart_uploads WHERE upload_id = $1;`)).
			WithArgs(uploadID).
			WillReturnRows(pgxmock.NewRows([]string{"status"}).AddRow("inited"))
		conn.ExpectExec(deleteQuery).
			WithArgs(uploadID).
			WillReturnResult(pgxmock.NewResult("DELETE", 10))
		conn.ExpectExec(updateQuery).
			WithArgs(services.MultipartStatusAborted, uploadID).
			WillReturnError(errors.New("db error"))
		conn.ExpectRollback()
		err = repo.ChangeUploadState(uploadID, services.MultipartStatusAborted)
		assert.Error(t, err)
	})
}

func TestAddUploadPart(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	repo := repos.NewMultipartRepoWithConn(conn)
	query := regexp.QuoteMeta(`INSERT INTO multipart_parts (upload_id, part_number, etag, size) VALUES ($1, $2, $3, $4);`)
	statusQuery := regexp.QuoteMeta(`SELECT status FROM multipart_uploads WHERE upload_id = $1;`)
	uploadID := uuid.New()
	part := models.UploadPart{
		Number: 1,
		Size:   16,
		Etag:   "\"test_etag\"",
	}
	t.Run("successfully added", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(statusQuery).
			WithArgs(uploadID).
			WillReturnRows(pgxmock.NewRows([]string{"status"}).AddRow(services.MultipartStatusInited))
		conn.ExpectExec(query).
			WithArgs(uploadID, part.Number, part.Etag, part.Size).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
		conn.ExpectCommit()
		err = repo.AddUploadPart(uploadID, &part)
		assert.NoError(t, err)
	})
	t.Run("FK violation", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(statusQuery).
			WithArgs(uploadID).
			WillReturnRows(pgxmock.NewRows([]string{"status"}).AddRow(services.MultipartStatusInited))
		conn.ExpectExec(query).
			WithArgs(uploadID, part.Number, part.Etag, part.Size).
			WillReturnError(&pgconn.PgError{
				Code: "23503",
			})
		conn.ExpectRollback()
		err = repo.AddUploadPart(uploadID, &part)
		assert.ErrorIs(t, err, errvalues.ErrUnexistUpload)
	})
	t.Run("Unique violation", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(statusQuery).
			WithArgs(uploadID).
			WillReturnRows(pgxmock.NewRows([]string{"status"}).AddRow(services.MultipartStatusInited))
		conn.ExpectExec(query).
			WithArgs(uploadID, part.Number, part.Etag, part.Size).
			WillReturnError(&pgconn.PgError{
				Code: "23505",
			})
		conn.ExpectRollback()
		err = repo.AddUploadPart(uploadID, &part)
		assert.ErrorIs(t, err, errvalues.ErrRepeatedUploadPart)
	})

	t.Run("db error", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(statusQuery).
			WithArgs(uploadID).
			WillReturnRows(pgxmock.NewRows([]string{"status"}).AddRow(services.MultipartStatusInited))
		conn.ExpectExec(query).
			WithArgs(uploadID, part.Number, part.Etag, part.Size).
			WillReturnError(errors.New("db error"))
		conn.ExpectRollback()
		err = repo.AddUploadPart(uploadID, &part)
		assert.Error(t, err)
	})
	t.Run("adding part to aborted upload", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(statusQuery).
			WithArgs(uploadID).
			WillReturnRows(pgxmock.NewRows([]string{"status"}).AddRow(services.MultipartStatusAborted))
		conn.ExpectRollback()
		err = repo.AddUploadPart(uploadID, &part)
		assert.ErrorIs(t, err, errvalues.ErrUploadAborted)
	})
	t.Run("adding part to completed upload", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectQuery(statusQuery).
			WithArgs(uploadID).
			WillReturnRows(pgxmock.NewRows([]string{"status"}).AddRow(services.MultipartStatusCompleted))
		conn.ExpectRollback()
		err = repo.AddUploadPart(uploadID, &part)
		assert.ErrorIs(t, err, errvalues.ErrUploadCompleted)
	})
}

func TestListUploads(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	repo := repos.NewMultipartRepoWithConn(conn)
	query := regexp.QuoteMeta(`SELECT u.upload_id, u.key, u.created_at FROM multipart_uploads u 
	INNER JOIN buckets b ON u.bucket_id = b.id 
	WHERE b.name = $1 AND owner_id = $2;`)
	ownerID := uuid.New()
	bucket := "test_bucket"
	uploads := make([]models.Upload, 0, 5)

	rows := pgxmock.NewRows([]string{"upload_id", "key", "created_at"})
	for i := range 5 {
		u := models.Upload{
			ID:        uuid.New(),
			Key:       fmt.Sprintf("key_n_%d.txt", i),
			Bucket:    bucket,
			OwnerID:   ownerID,
			CreatedAt: time.Now(),
			Status:    services.MultipartStatusInited,
		}
		uploads = append(uploads, u)
		rows.AddRow(u.ID, u.Key, u.CreatedAt, services.MultipartStatusInited)
	}

	t.Run("successful", func(t *testing.T) {
		conn.ExpectQuery(query).
			WithArgs(bucket, ownerID).
			WillReturnRows(rows)
		result, err := repo.ListUploads(ownerID, bucket)
		assert.NoError(t, err)
		for i := range len(uploads) {
			assert.Equal(t, uploads[i], *result[i])
		}
	})
	t.Run("db error", func(t *testing.T) {
		conn.ExpectQuery(query).
			WithArgs(bucket, ownerID).
			WillReturnError(errors.New("db error"))
		result, err := repo.ListUploads(ownerID, bucket)
		assert.Error(t, err)
		assert.True(t, result == nil)
	})
}

func TestListParts(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewPool()
	if err != nil {
		t.Fatal(err)
	}
	repo := repos.NewMultipartRepoWithConn(conn)
	query := regexp.QuoteMeta(`SELECT part_number, etag, size, created_at FROM multipart_parts WHERE upload_id = $1;`)
	uploadID := uuid.New()
	rows := pgxmock.NewRows([]string{"part_number", "etag", "size", "created_at"})
	parts := make([]models.UploadPart, 0, 5)
	for i := range 5 {
		p := models.UploadPart{
			Number:    i,
			Size:      int64((i + 1) * 2),
			Etag:      fmt.Sprintf("\"etag_n_%d\"", i),
			CreatedAt: time.Now(),
		}
		rows.AddRow(p.Number, p.Etag, p.Size, p.CreatedAt)
		parts = append(parts, p)
	}
	t.Run("successful", func(t *testing.T) {
		conn.ExpectQuery(query).
			WithArgs(uploadID).
			WillReturnRows(rows)
		result, err := repo.ListParts(uploadID)
		assert.NoError(t, err)
		for i := range len(result) {
			assert.Equal(t, parts[i], *result[i])
		}
	})
	t.Run("db error", func(t *testing.T) {
		conn.ExpectQuery(query).
			WithArgs(uploadID).
			WillReturnError(errors.New("db error"))
		_, err := repo.ListParts(uploadID)
		assert.Error(t, err)
	})
}

func TestMultipartIntegrational(t *testing.T) {
	t.Parallel()
	cfg := setupTestDB(t)
	repo := repos.NewMultipartRepo(cfg)
	bucket := "test_bucket"
	var err error
	{
		bucketRepo := repos.NewBucketRepo(cfg)
		_, err = bucketRepo.CreateBucket(ownerID, bucket)
		if err != nil {
			t.Fatal(err)
		}
	}
	upload := models.Upload{
		ID:      uuid.New(),
		Key:     "test_key.txt",
		Bucket:  bucket,
		OwnerID: ownerID,
	}
	parts := make([]models.UploadPart, 0, 5)
	for i := range 5 {
		parts = append(parts, models.UploadPart{
			Number:    i,
			Size:      int64((i + 1) * 2),
			Etag:      fmt.Sprintf("\"etag_n_%d\"", i),
			CreatedAt: time.Now(),
		})
	}
	t.Run("Init multipart upload", func(t *testing.T) {
		err = repo.CreateUpload(&upload)
		assert.NoError(t, err)
	})
	t.Run("Add few parts to upload", func(t *testing.T) {
		for _, p := range parts {
			err = repo.AddUploadPart(upload.ID, &p)
			assert.NoError(t, err)
		}
	})
	t.Run("list parts", func(t *testing.T) {
		result, err := repo.ListParts(upload.ID)
		assert.NoError(t, err)
		for i := range len(result) {
			assert.Equal(t, parts[i].Etag, result[i].Etag)
			assert.Equal(t, parts[i].Number, result[i].Number)
			assert.Equal(t, parts[i].Size, result[i].Size)
		}
	})
	t.Run("Complete upload", func(t *testing.T) {
		err = repo.ChangeUploadState(upload.ID, services.MultipartStatusCompleted)
		assert.NoError(t, err)
		// Check if upload completed
		err = repo.AddUploadPart(upload.ID, &parts[0])
		assert.ErrorIs(t, err, errvalues.ErrUploadCompleted)
		upload.Status = services.MultipartStatusCompleted
	})
	uploadToAbort := models.Upload{
		ID:      uuid.New(),
		Key:     "aborted.txt",
		Bucket:  bucket,
		OwnerID: ownerID,
	}
	t.Run("init another upload and abort it", func(t *testing.T) {
		err = repo.CreateUpload(&uploadToAbort)
		assert.NoError(t, err)
		err = repo.ChangeUploadState(uploadToAbort.ID, services.MultipartStatusAborted)
		assert.NoError(t, err)
		// Check if upload aborted
		err = repo.AddUploadPart(uploadToAbort.ID, &parts[0])
		assert.ErrorIs(t, err, errvalues.ErrUploadAborted)
		uploadToAbort.Status = services.MultipartStatusAborted
	})
	t.Run("list uploads", func(t *testing.T) {
		result, err := repo.ListUploads(ownerID, bucket)
		assert.NoError(t, err)
		assert.Equal(t, upload.ID, result[0].ID)
		assert.Equal(t, upload.Bucket, result[0].Bucket)
		assert.Equal(t, upload.Key, result[0].Key)
		assert.Equal(t, upload.Status, result[0].Status)
		assert.Equal(t, uploadToAbort.ID, result[1].ID)
		assert.Equal(t, uploadToAbort.Status, result[1].Status)
		assert.Equal(t, uploadToAbort.Bucket, result[1].Bucket)
		assert.Equal(t, uploadToAbort.Key, result[1].Key)
	})

}
