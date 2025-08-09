package repos

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lim-bo/barn/internal/errvalues"
	"github.com/lim-bo/barn/pkg/cleanup"
	"github.com/lim-bo/barn/pkg/models"
)

type ObjectsRepository struct {
	conn PgConnection
}

func NewObjectsRepo(cfg *DBConfig) *ObjectsRepository {
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
	return &ObjectsRepository{
		conn: p,
	}
}

func NewObjectsRepoWithConn(conn PgConnection) *ObjectsRepository {
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatal("ping error: " + err.Error())
	}
	return &ObjectsRepository{
		conn: conn,
	}
}

func (repo *ObjectsRepository) SaveObject(owner uuid.UUID, bucket string, obj *models.Object) error {
	var exists bool
	var bucketID uuid.UUID
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
	defer cancel()
	tx, err := repo.conn.Begin(ctx)
	if err != nil {
		return errors.New("starting tx error: " + err.Error())
	}
	defer tx.Rollback(ctx)
	err = tx.QueryRow(ctx, `SELECT id from buckets WHERE name = $1 AND owner_id = $2;`, bucket, owner).Scan(&bucketID)
	if err != nil {
		return errors.New("error getting bucket id: " + err.Error())
	}
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM objects WHERE bucket_id = $1 AND key = $2);`, bucketID, obj.Key).Scan(&exists)
	if err != nil {
		return errors.New("checking object existion error: " + err.Error())
	}
	if exists {
		_, err = tx.Exec(ctx, `UPDATE objects SET size = $1, etag = $2, last_modified = now() WHERE bucket_id = $3 AND key = $4;`,
			obj.Size, obj.Etag, bucketID, obj.Key)
		if err != nil {
			return errors.New("updating file error: " + err.Error())
		}
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO objects (bucket_id, key, size, etag) VALUES ($1, $2, $3, $4);`,
			bucketID, obj.Key, obj.Size, obj.Etag)
		if err != nil {
			return errors.New("inserting error: " + err.Error())
		}
	}
	err = tx.Commit(ctx)
	if err != nil {
		return errors.New("commiting error: " + err.Error())
	}
	return nil
}

func (repo *ObjectsRepository) GetObjectInfo(owner uuid.UUID, bucket, key string) (*models.Object, error) {
	obj := models.Object{
		Key: key,
	}
	query := `SELECT o.id, o.bucket_id, o.size, o.etag, o.last_modified FROM objects o INNER JOIN buckets b ON
o.bucket_id = b.id WHERE b.name = $1 AND o.key = $2 AND b.owner_id = $3;`
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	row := repo.conn.QueryRow(ctx, query, bucket, key, owner)
	if err := row.Scan(&obj.ID, &obj.BucketID, &obj.Size, &obj.Etag, &obj.LastModified); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errvalues.ErrUnexistObject
		}
		return nil, errors.New("getting obj info error: " + err.Error())
	}
	return &obj, nil
}

func (repo *ObjectsRepository) DeleteObject(owner uuid.UUID, bucket, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	query := `DELETE FROM objects USING buckets WHERE objects.bucket_id = buckets.id AND buckets.name = $1 AND objects.key = $2 AND buckets.owner_id = $3;`
	ct, err := repo.conn.Exec(ctx, query, bucket, key, owner)
	if err != nil {
		return errors.New("deleting error: " + err.Error())
	}
	if ct.RowsAffected() == 0 {
		return errvalues.ErrUnexistObject
	}
	return nil
}
