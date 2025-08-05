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
	"github.com/lim-bo/barn/pkg/cleanup"
	"github.com/lim-bo/barn/pkg/models"
)

type PgConnection interface {
	Ping(ctx context.Context) error
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type BucketsRepository struct {
	conn PgConnection
}

type DBConfig struct {
	Address  string
	User     string
	Password string
	DB       string
	Options  map[string]string
}

func NewBucketRepo(cfg *DBConfig) *BucketsRepository {
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
	return &BucketsRepository{
		conn: p,
	}
}

func NewBucketRepoWithConn(conn PgConnection) *BucketsRepository {
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatal("ping error: " + err.Error())
	}
	return &BucketsRepository{
		conn: conn,
	}
}

func (br *BucketsRepository) CreateBucket(ownerId uuid.UUID, bucket string) (*models.Bucket, error) {
	if !validateBucketName(bucket) {
		return nil, errvalues.ErrInvalidBucket
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tx, err := br.conn.Begin(ctx)
	if err != nil {
		return nil, errors.New("tx begin error: " + err.Error())
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `INSERT INTO buckets (name, owner_id) VALUES ($1, $2);`, bucket, ownerId)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			switch pgErr.Code {
			// FK voilation
			case "23503":
				return nil, errvalues.ErrNoUser
			// Unique violation
			case "23505":
				return nil, errvalues.ErrExistBucket
			}
		}
		return nil, errors.New("creating bucket error: " + err.Error())
	}
	var result models.Bucket
	result.Name = bucket
	result.OwnerID = ownerId
	err = tx.QueryRow(ctx, `SELECT id, created_at FROM buckets WHERE name = $1;`, bucket).Scan(&result.ID, &result.CreatedAt)
	if err != nil {
		return nil, errors.New("getting bucket info error: " + err.Error())
	}
	err = tx.Commit(ctx)
	if err != nil {
		return nil, errors.New("commiting tx error: " + err.Error())
	}
	return &result, nil
}

func (br *BucketsRepository) DeleteBucket(ownerId uuid.UUID, bucket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	ct, err := br.conn.Exec(ctx, `DELETE FROM buckets WHERE name = $1 AND owner_id = $2;`, bucket, ownerId)
	if err != nil {
		return errors.New("error deleting bucket: " + err.Error())
	}
	if ct.RowsAffected() == 0 {
		return errvalues.ErrNoBucket
	}
	return nil
}

func (br *BucketsRepository) ListAllBuckets(ownerId uuid.UUID) ([]*models.Bucket, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	rows, err := br.conn.Query(ctx, `SELECT id, name, owner_id, created_at FROM buckets WHERE owner_id = $1;`, ownerId)
	if err != nil {
		return nil, errors.New("error listing buckets: " + err.Error())
	}
	defer rows.Close()
	result := make([]*models.Bucket, 0, 10)
	for rows.Next() {
		buck := models.Bucket{}
		if err = rows.Scan(&buck.ID, &buck.Name, &buck.OwnerID, &buck.CreatedAt); err != nil {
			return nil, errors.New("parsing bucket row error: " + err.Error())
		}
		result = append(result, &buck)
	}
	return result, nil
}

func (br *BucketsRepository) CheckExist(ownerID uuid.UUID, bucket string) (bool, error) {
	var exist bool
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	err := br.conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM buckets WHERE owner_id = $1 AND name = $2);`, ownerID, bucket).Scan(&exist)
	if err != nil {
		return false, errors.New("checking bucket's existion error: " + err.Error())
	}
	return exist, nil
}
