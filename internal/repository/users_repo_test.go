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
	"github.com/lim-bo/barn/internal/services"
	"github.com/lim-bo/barn/pkg/models"
	"github.com/pashagolub/pgxmock/v2"
	"github.com/stretchr/testify/assert"
)

func TestCreate(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	ur := repos.NewUsersRepoWithConn(conn)
	username := "test_user"
	passwordHash := services.HashKey("password")
	user := models.User{
		ID:            uuid.New(),
		AccessKey:     services.GenerateAccessKey(),
		SecretKeyHash: services.HashKey(services.GenerateSecretKey()),
		Username:      &username,
		PasswordHash:  &passwordHash,
		Status:        "active",
		CreatedAt:     time.Now(),
	}
	query := regexp.QuoteMeta(`INSERT INTO users (access_key, secret_key_hash, username, password_hash, status) VALUES
	(
		$1, $2, $3, $4, $5
	);`)
	t.Run("successful", func(t *testing.T) {
		conn.ExpectExec(query).WithArgs(user.AccessKey, user.SecretKeyHash, user.Username, user.PasswordHash, user.Status).
			WillReturnResult(pgxmock.NewResult("INSERT", 1))
		err := ur.Create(&user)
		assert.NoError(t, err)
	})

	t.Run("with error", func(t *testing.T) {
		conn.ExpectExec(query).WithArgs(user.AccessKey, user.SecretKeyHash, user.Username, user.PasswordHash, user.Status).
			WillReturnError(errors.New("error"))
		err := ur.Create(&user)
		assert.Error(t, err)
	})
}

func TestGetWithKey(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	ur := repos.NewUsersRepoWithConn(conn)
	username := "test_user"
	passwordHash := services.HashKey("password")
	user := models.User{
		ID:            uuid.New(),
		AccessKey:     services.GenerateAccessKey(),
		SecretKeyHash: services.HashKey(services.GenerateSecretKey()),
		Username:      &username,
		PasswordHash:  &passwordHash,
		Status:        "active",
		CreatedAt:     time.Now(),
	}
	query := regexp.QuoteMeta(`SELECT id, secret_key_hash, username, password_hash, status, created_at FROM users
	WHERE access_key = $1;`)
	t.Run("successful", func(t *testing.T) {
		conn.ExpectQuery(query).WithArgs(user.AccessKey).
			WillReturnRows(pgxmock.NewRows([]string{"id", "secret_key_hash", "username", "password_hash", "status", "created_at"}).
				AddRow(user.ID, user.SecretKeyHash, user.Username, user.PasswordHash, user.Status, user.CreatedAt))
		result, err := ur.GetByAccessKey(user.AccessKey)
		assert.NoError(t, err)
		assert.EqualValues(t, user, *result)
	})
	t.Run("error no user", func(t *testing.T) {
		conn.ExpectQuery(query).WithArgs(user.AccessKey).WillReturnError(pgx.ErrNoRows)
		_, err := ur.GetByAccessKey(user.AccessKey)
		assert.ErrorIs(t, err, errvalues.ErrUnexistUser)
	})
	t.Run("db error", func(t *testing.T) {
		conn.ExpectQuery(query).WithArgs(user.AccessKey).WillReturnError(errors.New("error"))
		_, err := ur.GetByAccessKey(user.AccessKey)
		assert.Error(t, err)
	})
}

func TestGetWithUsername(t *testing.T) {
	t.Parallel()
	conn, err := pgxmock.NewConn()
	if err != nil {
		t.Fatal(err)
	}
	ur := repos.NewUsersRepoWithConn(conn)
	username := "test_user"
	passwordHash := services.HashKey("password")
	user := models.User{
		ID:            uuid.New(),
		AccessKey:     services.GenerateAccessKey(),
		SecretKeyHash: services.HashKey(services.GenerateSecretKey()),
		Username:      &username,
		PasswordHash:  &passwordHash,
		Status:        "active",
		CreatedAt:     time.Now(),
	}
	query := regexp.QuoteMeta(`SELECT id, access_key, secret_key_hash, password_hash, status, created_at FROM users
	WHERE username = $1;`)
	t.Run("successful", func(t *testing.T) {
		conn.ExpectQuery(query).WithArgs(*user.Username).
			WillReturnRows(pgxmock.NewRows([]string{"id", "access_key", "secret_key_hash", "password_hash", "status", "created_at"}).
				AddRow(user.ID, user.AccessKey, user.SecretKeyHash, user.PasswordHash, user.Status, user.CreatedAt))
		result, err := ur.GetByUsername(*user.Username)
		assert.NoError(t, err)
		assert.EqualValues(t, user, *result)
	})
	t.Run("error no user", func(t *testing.T) {
		conn.ExpectQuery(query).WithArgs(*user.Username).WillReturnError(pgx.ErrNoRows)
		_, err := ur.GetByUsername(*user.Username)
		assert.ErrorIs(t, err, errvalues.ErrUnexistUser)
	})
	t.Run("db error", func(t *testing.T) {
		conn.ExpectQuery(query).WithArgs(user.Username).WillReturnError(errors.New("error"))
		_, err := ur.GetByUsername(*user.Username)
		assert.Error(t, err)
	})
}

func TestUsersIntegrational(t *testing.T) {
	t.Parallel()
	cfg := setupTestDB(t)
	ur := repos.NewUsersRepo(&cfg)
	username := "test_user"
	passwordHash := services.HashKey("password")
	user := models.User{
		ID:            uuid.New(),
		AccessKey:     services.GenerateAccessKey(),
		SecretKeyHash: services.HashKey(services.GenerateSecretKey()),
		Username:      &username,
		PasswordHash:  &passwordHash,
		Status:        "active",
	}
	t.Run("create user", func(t *testing.T) {
		err := ur.Create(&user)
		assert.NoError(t, err)
	})
	t.Run("get user by accesskey", func(t *testing.T) {
		result, err := ur.GetByAccessKey(user.AccessKey)
		assert.NoError(t, err)
		assert.Equal(t, result.Username, user.Username)
		assert.Equal(t, result.SecretKeyHash, user.SecretKeyHash)
		assert.Equal(t, result.PasswordHash, user.PasswordHash)
		assert.Equal(t, result.Status, user.Status)
		assert.Equal(t, result.AccessKey, user.AccessKey)
	})
	t.Run("get user by username", func(t *testing.T) {
		result, err := ur.GetByUsername(*user.Username)
		assert.NoError(t, err)
		assert.Equal(t, result.Username, user.Username)
		assert.Equal(t, result.SecretKeyHash, user.SecretKeyHash)
		assert.Equal(t, result.PasswordHash, user.PasswordHash)
		assert.Equal(t, result.Status, user.Status)
		assert.Equal(t, result.AccessKey, user.AccessKey)
	})
}
