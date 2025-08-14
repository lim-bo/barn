package services

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lim-bo/barn/internal/errvalues"
	"github.com/lim-bo/barn/internal/services/pb"
	"github.com/lim-bo/barn/internal/storage"
	"github.com/lim-bo/barn/pkg/models"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	ObjectSizeHeader         = "content-lenght"
	ObjectETagHeader         = "etag"
	ObjectLastModifiedHeader = "last-modified"
)

type PaginationOpts struct {
	Offset int
	Limit  int
}

type ObjectRepository interface {
	SaveObject(owner uuid.UUID, bucket string, obj *models.Object) error
	GetObjectInfo(owner uuid.UUID, bucket, key string) (*models.Object, error)
	DeleteObject(owner uuid.UUID, bucket, key string) error
	ListObjects(owner uuid.UUID, bucket string, opts *PaginationOpts) ([]*models.Object, error)
}

type ObjectService struct {
	objRepo    ObjectRepository
	objStorage storage.ObjectStorage

	pb.UnimplementedObjectServiceServer
}

func NewObjectService(objRepo ObjectRepository, objStorage storage.ObjectStorage) *ObjectService {
	return &ObjectService{
		objRepo:    objRepo,
		objStorage: objStorage,
	}
}

func (os *ObjectService) LoadObject(ctx context.Context, req *pb.LoadObjectRequest) (*pb.LoadObjectResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("incoming unauthorized request")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	fileData := bytes.NewReader(req.Data)
	var etag string
	slog.Debug("values", slog.String("key", req.Key), slog.String("bucket", req.Bucket))
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		err = os.objStorage.SaveObject(context.Background(), ownerID.String()+"_"+req.Bucket, req.Key, fileData)
	}()
	go func() {
		defer wg.Done()
		etag = GenerateEtag(req.Data)
	}()
	wg.Wait()
	if err != nil {
		logger.Error("error saving file locally", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "error writing object to storage")
	}

	err = os.objRepo.SaveObject(ownerID, req.Bucket, &models.Object{
		Key:  req.Key,
		Size: uint64(fileData.Size()),
		Etag: etag,
	})
	if err != nil {
		logger.Error("error saving file info in db", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "error saving object info")
	}
	logger.Info("object loaded")
	return &pb.LoadObjectResponse{
		Etag: etag,
	}, nil
}

func (os *ObjectService) DeleteObject(ctx context.Context, req *pb.DeleteObjectRequest) (*pb.DeleteObjectResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("incoming unauthorized request")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	err = os.objStorage.DeleteObject(context.Background(), ownerID.String()+"_"+req.Bucket, req.Key)
	if err != nil {
		logger.Error("error deleting file locally", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to delete file")
	}
	err = os.objRepo.DeleteObject(ownerID, req.Bucket, req.Key)
	if err != nil {
		logger.Error("error deleting object info", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to delete object data")
	}
	logger.Info("object deleted")
	return &pb.DeleteObjectResponse{}, nil
}

func (os *ObjectService) GetObjectMD(ctx context.Context, req *pb.ObjectInfoRequest) (*pb.ObjectInfoResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("incoming unauthorized request")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	obj, err := os.objRepo.GetObjectInfo(ownerID, req.Bucket, req.Key)
	if err != nil {
		logger.Error("getting object info error", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to get object metadata")
	}
	md := metadata.New(map[string]string{
		ObjectLastModifiedHeader: obj.LastModified.Format(time.RFC3339),
		ObjectETagHeader:         obj.Etag,
		ObjectSizeHeader:         strconv.FormatUint(obj.Size, 10),
	})
	err = grpc.SendHeader(ctx, md)
	if err != nil {
		logger.Error("error sending headers", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to set headers")
	}
	logger.Info("object metadata provided")
	return &pb.ObjectInfoResponse{}, nil
}

func (os *ObjectService) GetObject(ctx context.Context, req *pb.GetObjectRequest) (*pb.GetObjectResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("incoming unauthorized request")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	reader, err := os.objStorage.GetObject(context.Background(), ownerID.String()+"_"+req.Bucket, req.Key)
	if err != nil {
		logger.Error("getting file data error", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to get file content")
	}
	defer reader.Close()
	data, err := io.ReadAll(reader)
	if err != nil {
		logger.Error("reading file data error", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to read data")
	}
	md := metadata.Pairs("Content-Type", "application/octet-stream")
	err = grpc.SendHeader(ctx, md)
	if err != nil {
		logger.Error("sending header error", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to set headers")
	}
	logger.Info("object provided")
	return &pb.GetObjectResponse{
		Data: data,
	}, nil
}

func (os *ObjectService) ListObjects(ctx context.Context, req *pb.ListObjectsRequest) (*pb.ListObjectsResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("incoming unauthorized request")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	var limit int
	if req.Limit == 0 {
		limit = math.MaxInt
	}
	objects, err := os.objRepo.ListObjects(ownerID, req.Bucket, &PaginationOpts{
		Offset: int(req.Offset),
		Limit:  limit,
	})
	if err != nil {
		logger.Error("getting objects error", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to get objects")
	}
	resp := pb.ListObjectsResponse{
		Bucket:  req.Bucket,
		Count:   int32(len(objects)),
		Limit:   req.Limit,
		Offset:  req.Offset,
		Content: make([]*pb.ObjectInfo, 0, len(objects)),
	}
	for _, o := range objects {
		resp.Content = append(resp.Content, &pb.ObjectInfo{
			Key:          o.Key,
			Size:         int64(o.Size),
			Etag:         o.Etag,
			LastModified: o.LastModified.Format(time.RFC3339),
		})
	}
	logger.Info("list of object provided")
	return &resp, nil
}
