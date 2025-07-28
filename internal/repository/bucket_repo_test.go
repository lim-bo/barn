package repos_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lim-bo/barn/internal/errvalues"
	repos "github.com/lim-bo/barn/internal/repository"
	"github.com/lim-bo/barn/pkg/models"
	"github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestCreateBucket(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	br := repos.NewBucketRepoWithConn(conn)
	expectedInsert := regexp.QuoteMeta(`INSERT INTO buckets (name, owner_id) VALUES ($1, $2);`)
	expectedSelect := regexp.QuoteMeta(`SELECT id, created_at FROM buckets WHERE name = $1;`)
	ownerID := uuid.New()
	bucket := "test_bucket"
	bucketID := uuid.New()
	created := time.Now()
	t.Run("successful", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectExec(expectedInsert).WithArgs(bucket, ownerID).WillReturnResult(pgxmock.NewResult("INSERT", 1))
		conn.ExpectQuery(expectedSelect).WithArgs(bucket).WillReturnRows(pgxmock.NewRows([]string{"id", "created_at"}).
			AddRow(bucketID, created))
		conn.ExpectCommit()
		b, err := br.CreateBucket(ownerID, bucket)
		assert.NoError(t, err)
		assert.Equal(t, models.Bucket{
			ID:        bucketID,
			Name:      bucket,
			OwnerID:   ownerID,
			CreatedAt: created,
		}, *b)
	})
	t.Run("insert error", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectExec(expectedInsert).WithArgs(bucket, ownerID).WillReturnError(errors.New("inserting error"))
		conn.ExpectRollback()
		_, err := br.CreateBucket(ownerID, bucket)
		assert.Error(t, err)
	})
	t.Run("select error", func(t *testing.T) {
		conn.ExpectBegin()
		conn.ExpectExec(expectedInsert).WithArgs(bucket, ownerID).WillReturnResult(pgxmock.NewResult("INSERT", 1))
		conn.ExpectQuery(expectedSelect).WithArgs(bucket).WillReturnError(errors.New("selecting error"))
		conn.ExpectRollback()
		_, err := br.CreateBucket(ownerID, bucket)
		assert.Error(t, err)
	})
}

func TestDeleteBucket(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	br := repos.NewBucketRepoWithConn(conn)
	ownerID := uuid.New()
	bucket := "test_bucket"
	expectedSql := regexp.QuoteMeta(`DELETE FROM buckets WHERE name = $1 AND owner_id = $2;`)
	t.Run("successful", func(t *testing.T) {
		conn.ExpectExec(expectedSql).WithArgs(bucket, ownerID).WillReturnResult(pgxmock.NewResult("DELETE", 1))
		err := br.DeleteBucket(ownerID, bucket)
		assert.NoError(t, err)
	})
	t.Run("delete error", func(t *testing.T) {
		conn.ExpectExec(expectedSql).WithArgs(bucket, ownerID).WillReturnError(errors.New("deleting error"))
		err := br.DeleteBucket(ownerID, bucket)
		assert.Error(t, err)
	})
	t.Run("no such bucket", func(t *testing.T) {
		conn.ExpectExec(expectedSql).WithArgs(bucket, ownerID).WillReturnResult(pgxmock.NewResult("DELETE", 0))
		err := br.DeleteBucket(ownerID, bucket)
		assert.ErrorIs(t, err, errvalues.ErrNoBucket)
	})
}

func TestListAllBuckets(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	br := repos.NewBucketRepoWithConn(conn)
	ownerID := uuid.New()
	buckets := []*models.Bucket{
		{
			ID:        uuid.New(),
			Name:      "bucket_1",
			OwnerID:   ownerID,
			CreatedAt: time.Now(),
		},
		{
			ID:        uuid.New(),
			Name:      "bucket_2",
			OwnerID:   ownerID,
			CreatedAt: time.Now(),
		},
		{
			ID:        uuid.New(),
			Name:      "bucket_3",
			OwnerID:   ownerID,
			CreatedAt: time.Now(),
		},
	}
	returned := pgxmock.NewRows([]string{"id", "name", "owner_id", "created_at"})
	for _, b := range buckets {
		returned.AddRow(b.ID, b.Name, b.OwnerID, b.CreatedAt)
	}
	expectedSql := regexp.QuoteMeta(`SELECT id, name, owner_id, created_at FROM buckets WHERE owner_id = $1;`)
	t.Run("successful", func(t *testing.T) {
		conn.ExpectQuery(expectedSql).WithArgs(ownerID).WillReturnRows(returned)
		result, err := br.ListAllBuckets(ownerID)
		assert.NoError(t, err)
		assert.True(t, func() bool {
			if len(result) != len(buckets) {
				return false
			}
			for i := range result {
				if !assert.ObjectsAreEqualValues(*buckets[i], *result[i]) {
					return false
				}
			}
			return true
		}())
	})
	t.Run("select error", func(t *testing.T) {
		conn.ExpectQuery(expectedSql).WithArgs(ownerID).WillReturnError(errors.New("selecting error"))
		_, err := br.ListAllBuckets(ownerID)
		assert.Error(t, err)
	})
}

var ownerID = uuid.New()

func TestIntegrational(t *testing.T) {
	t.Parallel()
	cfg := setupTestDB(t)
	br := repos.NewBucketRepo(&cfg)
	bucket := "test_bucket"
	t.Run("creating buckets", func(t *testing.T) {
		for i := range 4 {
			b, err := br.CreateBucket(ownerID, fmt.Sprintf("%s №%d", bucket, i))
			assert.NoError(t, err)
			t.Log("bucket created: ", b)
		}
	})
	t.Run("deleting bucket", func(t *testing.T) {
		err := br.DeleteBucket(ownerID, fmt.Sprintf("%s №%d", bucket, 3))
		assert.NoError(t, err)
	})
	t.Run("listing bucket", func(t *testing.T) {
		buckets, err := br.ListAllBuckets(ownerID)
		assert.NoError(t, err)
		for _, b := range buckets {
			t.Log(b)
		}
	})
}

func setupTestDB(t *testing.T) repos.DBConfig {
	container, err := postgres.Run(context.Background(), "postgres:17",
		postgres.WithUsername("test_user"),
		postgres.WithDatabase("barn"),
		postgres.WithPassword("test_password"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatal("error running test container: " + err.Error())
	}
	connStr, err := container.ConnectionString(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	pgxpoolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		t.Fatal("error parsing config: " + err.Error())
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), pgxpoolCfg)
	if err != nil {
		t.Fatal("error connecting to container: " + err.Error())
	}
	migrations, err := os.ReadFile("../../migrations/postgresql/baseline.sql")
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(context.Background(), string(migrations))
	if err != nil {
		t.Fatal("error setting migrations: " + err.Error())
	}
	// Inserting user for owning test buckets
	_, err = pool.Exec(context.Background(), `INSERT INTO users (id, access_key, secret_key_hash) VALUES ($1, $2, $3);`, ownerID, "12345", "12345")
	if err != nil {
		t.Fatal("error setting migrations: " + err.Error())
	}
	pool.Close()
	t.Cleanup(func() {
		container.Terminate(context.Background())
	})
	return repos.DBConfig{
		Address:  pgxpoolCfg.ConnConfig.Host + ":" + strconv.FormatUint(uint64(pgxpoolCfg.ConnConfig.Port), 10),
		Password: "test_password",
		User:     "test_user",
		DB:       "barn",
	}
}
