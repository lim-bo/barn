package repos

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lim-bo/barn/internal/errvalues"
	"github.com/lim-bo/barn/internal/services"
	"github.com/lim-bo/barn/pkg/cleanup"
	"github.com/lim-bo/barn/pkg/models"
)

type MultipartRepository struct {
	conn PgConnection
}

func NewMultipartRepo(cfg *DBConfig) *MultipartRepository {
	optsStr := ""
	if len(cfg.Options) != 0 {
		optsStr = "?"
		for k, v := range cfg.Options {
			optsStr += k + "=" + v
		}
	}
	p, err := pgxpool.New(context.Background(), "postgresql://"+cfg.User+":"+cfg.Password+"@"+cfg.Address+"/"+cfg.DB+optsStr)
	if err != nil {
		log.Fatal(err)
	}
	err = p.Ping(context.Background())
	if err != nil {
		log.Fatal("ping error: " + err.Error())
	}
	cleanup.Register(&cleanup.Job{
		Name: "closing psql conn",
		Func: func() error {
			p.Close()
			return nil
		},
	})
	return &MultipartRepository{
		conn: p,
	}
}

func NewMultipartRepoWithConn(conn PgConnection) *MultipartRepository {
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatal("ping error: " + err.Error())
	}
	return &MultipartRepository{
		conn: conn,
	}
}

// ID, ownerID, bucket, key in upload is required
func (repo *MultipartRepository) CreateUpload(upload *models.Upload) error {
	var bucketID uuid.UUID
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	tx, err := repo.conn.Begin(ctx)
	if err != nil {
		return errors.New("error starting tx: " + err.Error())
	}
	defer tx.Rollback(ctx)

	row := tx.QueryRow(ctx, `SELECT id FROM buckets WHERE name = $1 AND owner_id = $2;`, upload.Bucket, upload.OwnerID)
	if err = row.Scan(&bucketID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errvalues.ErrNoBucket
		}
		return errors.New("error getting bucketID: " + err.Error())
	}

	_, err = tx.Exec(ctx, `INSERT INTO multipart_uploads (upload_id, bucket_id, key, status) VALUES ($1, $2, $3, $4);`,
		upload.ID, bucketID, upload.Key, services.MultipartStatusInited)
	if err != nil {
		return errors.New("error creating new upload: " + err.Error())
	}

	err = tx.Commit(ctx)
	if err != nil {
		return errors.New("commit error: " + err.Error())
	}
	return nil
}

// State is Completed or Aborted
func (repo *MultipartRepository) ChangeUploadState(uploadID uuid.UUID, state string) error {
	if state != services.MultipartStatusAborted && state != services.MultipartStatusCompleted {
		return errors.New("invalid state const")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	tx, err := repo.conn.Begin(ctx)
	if err != nil {
		return errors.New("error starting tx: " + err.Error())
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `DELETE FROM multipart_parts WHERE upload_id = $1;`, uploadID)
	if err != nil {
		return errors.New("error deleting upload's parts: " + err.Error())
	}

	ct, err := tx.Exec(ctx, `UPDATE multipart_uploads SET status = $1 WHERE upload_id = $2;`, state, uploadID)
	if err != nil {
		return errors.New("error updating upload status: " + err.Error())
	}
	if ct.RowsAffected() == 0 {
		return errvalues.ErrUnexistUpload
	}

	err = tx.Commit(ctx)
	if err != nil {
		return errors.New("commit error: " + err.Error())
	}
	return nil
}

func (repo *MultipartRepository) AddUploadPart(uploadID uuid.UUID, part *models.UploadPart) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tx, err := repo.conn.Begin(ctx)
	if err != nil {
		return errors.New("error starting tx: " + err.Error())
	}
	defer tx.Rollback(ctx)
	var status string
	if err = tx.QueryRow(ctx, `SELECT status FROM multipart_uploads WHERE upload_id = $1;`, uploadID).Scan(&status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errvalues.ErrUnexistUpload
		}
		return errors.New("error getting status: " + err.Error())
	}

	switch status {
	case services.MultipartStatusAborted:
		return errvalues.ErrUploadAborted
	case services.MultipartStatusCompleted:
		return errvalues.ErrUploadCompleted
	default:
		break
	}
	_, err = tx.Exec(ctx, `INSERT INTO multipart_parts (upload_id, part_number, etag, size) VALUES ($1, $2, $3, $4);`,
		uploadID, part.Number, part.Etag, part.Size)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			// FK voilation
			case "23503":
				return errvalues.ErrUnexistUpload
			// Unique violation
			case "23505":
				return errvalues.ErrRepeatedUploadPart
			}
		}
		return errors.New("error adding upload part: " + err.Error())
	}
	err = tx.Commit(ctx)
	if err != nil {
		return errors.New("error commiting tx: " + err.Error())
	}
	return nil
}

func (repo *MultipartRepository) ListUploads(ownerID uuid.UUID, bucket string) ([]*models.Upload, error) {
	result := make([]*models.Upload, 0, 2)
	query := `SELECT u.upload_id, u.key, u.created_at, u.status FROM multipart_uploads u 
	INNER JOIN buckets b ON u.bucket_id = b.id 
	WHERE b.name = $1 AND owner_id = $2;`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	rows, err := repo.conn.Query(ctx, query, bucket, ownerID)
	if err != nil {
		return nil, errors.New("error getting upload list: " + err.Error())
	}
	for rows.Next() {
		u := models.Upload{}
		err = rows.Scan(&u.ID, &u.Key, &u.CreatedAt, &u.Status)
		if err != nil {
			return nil, errors.New("error unmarshalling results: " + err.Error())
		}
		u.Bucket = bucket
		u.OwnerID = ownerID
		result = append(result, &u)
	}
	if rows.Err() != nil {
		return nil, errors.New("unexpected error after scanning: " + err.Error())
	}
	return result, nil
}

func (repo *MultipartRepository) ListParts(uploadID uuid.UUID) ([]*models.UploadPart, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	rows, err := repo.conn.Query(ctx, `SELECT part_number, etag, size, created_at FROM multipart_parts WHERE upload_id = $1;`, uploadID)
	if err != nil {
		return nil, errors.New("getting parts error: " + err.Error())
	}
	result := make([]*models.UploadPart, 0, 2)
	for rows.Next() {
		part := models.UploadPart{}
		err = rows.Scan(&part.Number, &part.Etag, &part.Size, &part.CreatedAt)
		if err != nil {
			return nil, errors.New("error unmarshalling results: " + err.Error())
		}
		result = append(result, &part)
	}
	if rows.Err() != nil {
		return nil, errors.New("unexpected error after scanning: " + err.Error())
	}
	return result, nil
}
