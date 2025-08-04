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
	"github.com/lim-bo/barn/internal/cleanup"
	"github.com/lim-bo/barn/internal/errvalues"
	"github.com/lim-bo/barn/pkg/models"
)

type UsersRepository struct {
	conn PgConnection
}

func NewUsersRepo(cfg *DBConfig) *UsersRepository {
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
	return &UsersRepository{
		conn: p,
	}
}

func NewUsersRepoWithConn(conn PgConnection) *UsersRepository {
	if err := conn.Ping(context.Background()); err != nil {
		log.Fatal("ping error: " + err.Error())
	}
	return &UsersRepository{
		conn: conn,
	}
}

func (ur *UsersRepository) Create(user *models.User) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err := ur.conn.Exec(ctx, `INSERT INTO users (access_key, secret_key_hash, username, password_hash, status) VALUES
	(
		$1, $2, $3, $4, $5
	);`, user.AccessKey, user.SecretKeyHash, user.Username, user.PasswordHash, user.Status)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			return errvalues.ErrUserExists
		}
		return errors.New("inserting user error: " + err.Error())
	}
	return nil
}

func (ur *UsersRepository) GetByAccessKey(accessKey string) (*models.User, error) {
	var user models.User
	user.AccessKey = accessKey
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	err := ur.conn.QueryRow(ctx, `SELECT id, secret_key_hash, username, password_hash, status, created_at FROM users
	WHERE access_key = $1;`, accessKey).Scan(&user.ID, &user.SecretKeyHash, &user.Username, &user.PasswordHash, &user.Status, &user.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errvalues.ErrUnexistUser
		}
		return nil, errors.New("getting user error: " + err.Error())
	}
	return &user, nil
}

func (ur *UsersRepository) GetByUsername(username string) (*models.User, error) {
	var user models.User
	user.Username = &username
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	err := ur.conn.QueryRow(ctx, `SELECT id, access_key, secret_key_hash, password_hash, status, created_at FROM users
	WHERE username = $1;`, username).Scan(&user.ID, &user.AccessKey, &user.SecretKeyHash, &user.PasswordHash, &user.Status, &user.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errvalues.ErrUnexistUser
		}
		return nil, errors.New("getting user error: " + err.Error())
	}
	return &user, nil
}

func (ur *UsersRepository) UpdateKeys(uid uuid.UUID, accessKey string, secretKeyHash string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	ct, err := ur.conn.Exec(ctx, `UPDATE users SET access_key = $1, secret_key_hash = $2 WHERE id = $3;`, accessKey, secretKeyHash, uid)
	if err != nil {
		return errors.New("updating keys error: " + err.Error())
	}
	if ct.RowsAffected() == 0 {
		return errvalues.ErrUnexistUser
	}
	return nil
}
