package services_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/lib/pq"
	"github.com/pressly/goose/v3"

	"github.com/google/uuid"
	repos "github.com/lim-bo/barn/internal/repository"
	"github.com/lim-bo/barn/internal/services"
	"github.com/lim-bo/barn/internal/services/pb"
	"github.com/lim-bo/barn/internal/storage"
	"github.com/lim-bo/barn/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/genproto/googleapis/api/httpbody"
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

	s := grpc.NewServer(grpc.ChainUnaryInterceptor(
		services.RequestIDInterceptor,
		services.LoggerSettingInterceptor(slog.Default())))
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
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(
		services.RequestIDInterceptor,
		authInterceptorPlaceholder,
		services.LoggerSettingInterceptor(slog.Default())))
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
	bucketName := "test_bucket"
	ctx := context.Background()
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

func TestObjectService(t *testing.T) {
	t.Parallel()
	// Setting up listener and repository on testcontainer
	lis := bufconn.Listen(bufSize)
	cfg := setupTestDB(t)
	or := repos.NewObjectsRepo(cfg)
	oStorage := storage.NewLocalFS("../../data")

	// Registering new server
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(
		services.RequestIDInterceptor,
		authInterceptorPlaceholder,
		services.LoggerSettingInterceptor(slog.Default())))
	os := services.NewObjectService(services.ObjectServiceConfig{
		ObjRepo:    or,
		ObjStorage: oStorage,
	})
	pb.RegisterObjectServiceServer(s, os)

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
	ctx := context.Background()
	client := pb.NewObjectServiceClient(conn)
	bStorage := storage.NewBucketLocalFS("../../data")
	bucket := "test_bucket"
	{
		bStorage.CreateBucket(ctx, ownerID.String()+"_"+bucket)
		bRepo := repos.NewBucketRepo(cfg)
		_, err := bRepo.CreateBucket(ownerID, bucket)
		if err != nil {
			t.Fatal(err)
		}
	}

	requests := make([]*pb.LoadObjectRequest, 0, 5)
	keys := make([]string, 0, 5)
	filesContent := make([][]byte, 0, 5)
	for i := range 5 {
		key := fmt.Sprintf("key_n_%d.txt", i)
		data := fmt.Appendf(nil, "data of file n_%d", i)
		requests = append(requests, &pb.LoadObjectRequest{
			Bucket: bucket,
			Key:    key,
			Body: &httpbody.HttpBody{
				ContentType: "application/octet-stream",
				Data:        data,
			},
		})
		keys = append(keys, key)
		filesContent = append(filesContent, slices.Clone(data))
	}
	objects := make([]*models.Object, 0, 5)
	t.Run("Saving objects", func(t *testing.T) {
		for _, req := range requests {
			_, err := client.LoadObject(ctx, req)
			assert.NoError(t, err)
		}
	})
	t.Run("Getting files' metadata", func(t *testing.T) {
		for i := range 5 {
			md := metadata.MD{}
			_, err := client.GetObjectMD(ctx, &pb.ObjectInfoRequest{
				Bucket: bucket,
				Key:    keys[i],
			}, grpc.Header(&md))
			assert.NoError(t, err)

			size, err := strconv.ParseUint(md.Get(services.ObjectSizeHeader)[0], 10, 64)
			assert.NoError(t, err)

			etag := md.Get(services.ObjectETagHeader)[0]

			lastModified, err := time.Parse(time.RFC3339, md.Get(services.ObjectLastModifiedHeader)[0])
			assert.NoError(t, err)
			t.Logf("object n_%d: etag: %s, size: %d, lastModified: %v", i, etag, size, lastModified)
			objects = append(objects, &models.Object{
				Size:         size,
				Etag:         etag,
				LastModified: lastModified,
			})
		}
	})
	t.Run("Deleting object", func(t *testing.T) {
		_, err := client.DeleteObject(ctx, &pb.DeleteObjectRequest{
			Bucket: bucket,
			Key:    keys[4],
		})
		assert.NoError(t, err)
		keys = keys[:4]
	})
	t.Run("Getting object data", func(t *testing.T) {
		resp, err := client.GetObject(ctx, &pb.GetObjectRequest{
			Bucket: bucket,
			Key:    keys[0],
		})
		assert.NoError(t, err)
		assert.True(t, slices.Equal(resp.Data, filesContent[0]))
	})
	t.Run("Listing objects", func(t *testing.T) {
		limit, offset := 3, 1
		resp, err := client.ListObjects(ctx, &pb.ListObjectsRequest{
			Bucket: bucket,
			Limit:  int32(limit),
			Offset: int32(offset),
		})
		assert.NoError(t, err)
		assert.Equal(t, bucket, resp.Bucket)
		assert.EqualValues(t, 3, resp.Count)
		assert.EqualValues(t, limit, resp.Limit)
		assert.EqualValues(t, offset, resp.Offset)
		for i, o := range resp.Content {
			assert.True(t, o.Etag == objects[i+offset].Etag)
			assert.True(t, o.Size == int64(objects[i+offset].Size))
			assert.True(t, o.Key == keys[i+offset])
		}
	})
	bStorage.DeleteBucket(ctx, bucket)
}

func TestObjectServiceMultipart(t *testing.T) {
	t.Parallel()
	// Setting up listener and repository on testcontainer
	lis := bufconn.Listen(bufSize)
	cfg := setupTestDB(t)
	multStorage := storage.NewMultipartLocalFS("../../data")
	multRepo := repos.NewMultipartRepo(cfg)
	objRepo := repos.NewObjectsRepo(cfg)
	objStorage := storage.NewLocalFS("../../data")
	// Registering new server
	s := grpc.NewServer(grpc.ChainUnaryInterceptor(
		services.RequestIDInterceptor,
		authInterceptorPlaceholder,
		services.LoggerSettingInterceptor(slog.Default())))
	os := services.NewObjectService(services.ObjectServiceConfig{
		MultipartRepo:    multRepo,
		MultipartStorage: multStorage,
		ObjRepo:          objRepo,
		ObjStorage:       objStorage,
	})
	pb.RegisterObjectServiceServer(s, os)

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
	ctx := context.Background()
	client := pb.NewObjectServiceClient(conn)

	bucket := "test_bucket"
	{
		bStorage := storage.NewBucketLocalFS("../../data")
		bStorage.CreateBucket(ctx, ownerID.String()+"_"+bucket)
		bRepo := repos.NewBucketRepo(cfg)
		_, err := bRepo.CreateBucket(ownerID, bucket)
		if err != nil {
			t.Fatal(err)
		}
	}

	key := "big_file.data"
	fileData := make([]byte, 128)
	chunkSize := 32
	chunkCount := len(fileData) / chunkSize
	_, err = rand.Read(fileData)
	if err != nil {
		t.Fatal(err)
	}
	var uploadID uuid.UUID
	t.Run("Inited multipart upload", func(t *testing.T) {
		resp, err := client.InitMultipart(ctx, &pb.InitMultipartRequest{
			Bucket: bucket,
			Key:    key,
		})
		assert.NoError(t, err)
		uploadID = uuid.MustParse(resp.UploadId)
	})
	etags := make([]string, 0, chunkCount)
	t.Run("Adding few parts", func(t *testing.T) {
		for i := range chunkCount {
			offset := i * chunkSize
			resp, err := client.UploadPart(ctx, &pb.UploadPartRequest{
				Bucket:     bucket,
				Key:        key,
				UploadId:   uploadID.String(),
				PartNumber: int32(i),
				Body: &httpbody.HttpBody{
					ContentType: "application/octet-stream",
					Data:        fileData[offset : offset+chunkSize],
				}})
			assert.NoError(t, err)
			etags = append(etags, resp.Etag)
		}
	})
	t.Run("Listing parts", func(t *testing.T) {
		resp, err := client.ListParts(ctx, &pb.ListPartsRequest{
			Bucket:   bucket,
			Key:      key,
			UploadId: uploadID.String(),
		})
		assert.NoError(t, err)
		assert.Equal(t, bucket, resp.Bucket)
		assert.Equal(t, key, resp.Key)
		assert.Equal(t, uploadID.String(), resp.UploadId)
		for i := range resp.Parts {
			assert.Equal(t, etags[i], resp.Parts[i].Etag)
			assert.EqualValues(t, i, resp.Parts[i].PartNumber)
			assert.EqualValues(t, chunkSize, resp.Parts[i].Size)
		}
	})
	t.Run("Completing upload", func(t *testing.T) {
		reqParts := make([]*pb.CompletedPart, 0, chunkCount)
		for i := range chunkCount {
			reqParts = append(reqParts, &pb.CompletedPart{
				PartNumber: int32(i),
				Etag:       etags[i],
			})
		}
		resp, err := client.CompleteMultipart(ctx, &pb.CompleteMultipartRequest{
			Bucket:   bucket,
			Key:      key,
			UploadId: uploadID.String(),
			Parts:    reqParts,
		})
		assert.NoError(t, err)
		assert.EqualValues(t, chunkCount, resp.PartCount)
		fmt.Printf("result etag: %s", resp.Etag)
	})
	t.Run("getting file content", func(t *testing.T) {
		resp, err := client.GetObject(ctx, &pb.GetObjectRequest{
			Bucket: bucket,
			Key:    key,
		})
		assert.NoError(t, err)
		assert.Equal(t, fileData, resp.Data)
	})
	var uploadIDAborted uuid.UUID
	keyAborted := "big_file_to_abort.txt"
	t.Run("Creating upload", func(t *testing.T) {
		resp, err := client.InitMultipart(ctx, &pb.InitMultipartRequest{
			Bucket: bucket,
			Key:    keyAborted,
		})
		assert.NoError(t, err)
		uploadIDAborted = uuid.MustParse(resp.UploadId)
	})
	t.Run("Aborting created upload", func(t *testing.T) {
		_, err := client.AbortMultipart(ctx, &pb.AbortMultipartRequest{
			Bucket:   bucket,
			Key:      keyAborted,
			UploadId: uploadIDAborted.String(),
		})
		assert.NoError(t, err)
	})
	t.Run("Listing uploads", func(t *testing.T) {
		resp, err := client.ListMultipartUploads(ctx, &pb.ListMultipartUploadsRequest{
			Bucket: bucket,
		})
		assert.NoError(t, err)
		completed := resp.Uploads[0]
		aborted := resp.Uploads[1]
		assert.Equal(t, completed.Key, key)
		assert.Equal(t, completed.UploadId, uploadID.String())
		assert.Equal(t, aborted.Key, keyAborted)
		assert.Equal(t, aborted.UploadId, uploadIDAborted.String())
	})
}

func authInterceptorPlaceholder(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	ctx = context.WithValue(ctx, "Owner-ID", ownerID.String())
	return handler(ctx, req)
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
	connStr += "sslmode=disable"
	conn, err := sql.Open("postgres", connStr)
	if err != nil {
		t.Fatal(err)
	}
	err = goose.Up(conn, "../../migrations/postgresql")
	if err != nil {
		t.Fatal(err)
	}

	// Inserting user for owning test buckets
	_, err = conn.Exec(`INSERT INTO users (id, access_key, secret_key_hash) VALUES ($1, $2, $3);`, ownerID, "12345", "12345")
	if err != nil {
		t.Fatal("error setting migrations: " + err.Error())
	}
	conn.Close()
	t.Cleanup(func() {
		container.Terminate(context.Background())
	})
	pgxpoolCfg, err := pgxpool.ParseConfig(connStr)
	return &repos.DBConfig{
		Address:  pgxpoolCfg.ConnConfig.Host + ":" + strconv.FormatUint(uint64(pgxpoolCfg.ConnConfig.Port), 10),
		Password: "test_password",
		User:     "test_user",
		DB:       "barn",
	}
}
