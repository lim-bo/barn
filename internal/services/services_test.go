package services_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	repos "github.com/lim-bo/barn/internal/repository"
	"github.com/lim-bo/barn/internal/services"
	"github.com/lim-bo/barn/internal/services/pb"
	"github.com/lim-bo/barn/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	bufSize = 1024 * 1024
	ownerID = uuid.New()
)

func TestAuthService(t *testing.T) {
	t.Parallel()
	lis := bufconn.Listen(bufSize)
	ur := repos.NewUsersRepo(setupTestDB(t))

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(services.RequestIDInterceptor))
	as := services.NewAuthService(ur)
	pb.RegisterAuthServiceServer(s, as)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Fatal(err)
		}
	}()
	t.Cleanup(func() {
		s.GracefulStop()
	})

	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(
		func(ctx context.Context, s string) (net.Conn, error) {
			return lis.Dial()
		},
	), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	client := pb.NewAuthServiceClient(conn)
	credentials := map[string]string{
		"username": "test_user",
		"password": "test_password",
	}
	ctx := context.Background()
	t.Run("register with keys", func(t *testing.T) {
		resp, err := client.RegisterWithKeys(ctx, &emptypb.Empty{})
		assert.NoError(t, err)
		t.Logf("recieved keys: access: %s secret %s", resp.Keys.AccessKey, resp.Keys.SecretKey)
	})
	t.Run("register with credentials", func(t *testing.T) {
		resp, err := client.RegisterWithPassword(ctx, &pb.RegisterWithPasswordRequest{
			Credentials: &pb.UserCredentials{
				Username: credentials["username"],
				Password: credentials["password"],
			},
		})
		assert.NoError(t, err)
		t.Logf("recieved keys: access: %s secret %s", resp.Keys.AccessKey, resp.Keys.SecretKey)
	})
	t.Run("login via password", func(t *testing.T) {
		resp, err := client.LoginWithPassword(ctx, &pb.LoginRequest{
			Credentials: &pb.UserCredentials{
				Username: credentials["username"],
				Password: credentials["password"],
			},
		})
		assert.NoError(t, err)
		t.Logf("renewed keys: access: %s secret %s", resp.Keys.AccessKey, resp.Keys.SecretKey)
	})
}

func TestBucketService(t *testing.T) {
	t.Parallel()
	// Setting up listener and repository on testcontainer
	lis := bufconn.Listen(bufSize)
	br := repos.NewBucketRepo(setupTestDB(t))
	bStorage := storage.NewBucketLocalFS("../../data")
	// Registering new server
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(services.RequestIDInterceptor, services.AuthInterceptor))
	bs := services.NewBucketService(br, bStorage)
	pb.RegisterBucketServiceServer(s, bs)

	go func() {
		if err := s.Serve(lis); err != nil {
			t.Fatal(err)
		}
	}()

	t.Cleanup(func() {
		s.GracefulStop()
	})

	// Creating client to server
	conn, err := grpc.NewClient("passthrough:///bufnet", grpc.WithContextDialer(
		func(ctx context.Context, s string) (net.Conn, error) {
			return lis.Dial()
		},
	), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		conn.Close()
	})

	// Setting up data to operate with
	md := metadata.Pairs("authorization", ownerID.String())
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	bucketName := "test_bucket"

	client := pb.NewBucketServiceClient(conn)
	t.Run("Creating bucket", func(t *testing.T) {
		for i := range 4 {
			b, err := client.CreateBucket(ctx, &pb.CreateBucketRequest{
				Name: fmt.Sprintf("%s_%d", bucketName, i),
			})
			assert.NoError(t, err)
			assert.Equal(t, fmt.Sprintf("%s_%d", bucketName, i), b.Bucket.Name)
		}
	})
	t.Run("Checking if bucket exis: true", func(t *testing.T) {
		_, err := client.CheckExistBucket(ctx, &pb.CheckExistBucketRequest{
			Name: fmt.Sprintf("%s_%d", bucketName, 0),
		})
		assert.NoError(t, err)
	})
	t.Run("Deleting bucket", func(t *testing.T) {
		_, err := client.DeleteBucket(ctx, &pb.DeleteBucketRequest{
			Name: fmt.Sprintf("%s_%d", bucketName, 3),
		})
		assert.NoError(t, err)
	})
	t.Run("Checking if bucket exis: false", func(t *testing.T) {
		_, err := client.CheckExistBucket(ctx, &pb.CheckExistBucketRequest{
			Name: fmt.Sprintf("%s_%d", bucketName, 3),
		})
		assert.Error(t, err)
	})
	t.Run("Listing buckets", func(t *testing.T) {
		result, err := client.ListAllBuckets(ctx, &pb.ListAllBucketsRequest{})
		assert.NoError(t, err)
		for i, b := range result.Buckets {
			assert.Equal(t, fmt.Sprintf("%s_%d", bucketName, i), b.Name)
		}
	})
	t.Run("Deleting rest", func(t *testing.T) {
		for i := range 3 {
			_, err := client.DeleteBucket(ctx, &pb.DeleteBucketRequest{
				Name: fmt.Sprintf("%s_%d", bucketName, i),
			})
			assert.NoError(t, err)
		}
	})
}

func setupTestDB(t *testing.T) *repos.DBConfig {
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
	migrations, err := os.ReadFile("../../migrations/postgresql/0_baseline.sql")
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(context.Background(), string(migrations))
	if err != nil {
		t.Fatal("error setting migrations: " + err.Error())
	}
	migrations, err = os.ReadFile("../../migrations/postgresql/1_add_username_password.up.sql")
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
	return &repos.DBConfig{
		Address:  pgxpoolCfg.ConnConfig.Host + ":" + strconv.FormatUint(uint64(pgxpoolCfg.ConnConfig.Port), 10),
		Password: "test_password",
		User:     "test_user",
		DB:       "barn",
	}
}
