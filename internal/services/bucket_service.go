package services

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/lim-bo/barn/internal/errvalues"
	"github.com/lim-bo/barn/internal/services/pb"
	"github.com/lim-bo/barn/internal/storage"
	"github.com/lim-bo/barn/pkg/models"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BucketService struct {
	bucketsRepo   BucketsRepositoryI
	storageEngine storage.BucketStorage

	pb.UnimplementedBucketServiceServer
}

type BucketsRepositoryI interface {
	CreateBucket(ownerId uuid.UUID, bucket string) (*models.Bucket, error)
	DeleteBucket(ownerId uuid.UUID, bucket string) error
	ListAllBuckets(ownerId uuid.UUID) ([]*models.Bucket, error)
	CheckExist(ownerID uuid.UUID, bucket string) (bool, error)
}

func NewBucketService(br BucketsRepositoryI, bucketStorage storage.BucketStorage) *BucketService {
	return &BucketService{
		bucketsRepo:   br,
		storageEngine: bucketStorage,
	}
}

func (bs *BucketService) CreateBucket(ctx context.Context, req *pb.CreateBucketRequest) (*pb.CreateBucketResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("create bucket request with invalid uid")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	bucket, err := bs.bucketsRepo.CreateBucket(ownerID, req.Name)
	if err != nil {
		logger.Error("error creating bucket", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, err.Error())
	}
	go func() {
		err := bs.storageEngine.CreateBucket(context.Background(), ownerID.String()+"_"+req.Name)
		if err != nil {
			logger.Error("error created bucket in storage", slog.String("error", err.Error()))
		}
	}()
	logger.Info("successfully created bucket")
	return &pb.CreateBucketResponse{
		Bucket: &pb.Bucket{
			Id:        bucket.ID.String(),
			Name:      bucket.Name,
			OwnerId:   bucket.OwnerID.String(),
			CreatedAt: bucket.CreatedAt.String(),
		},
	}, nil
}

func (bs *BucketService) DeleteBucket(ctx context.Context, req *pb.DeleteBucketRequest) (*pb.DeleteBucketResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("delete bucket request with invalid uid")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	err = bs.bucketsRepo.DeleteBucket(ownerID, req.Name)
	if err != nil {
		logger.Error("error deleting bucket", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, err.Error())
	}
	go func() {
		err := bs.storageEngine.DeleteBucket(context.Background(), ownerID.String()+"_"+req.Name)
		if err != nil {
			logger.Error("error deleting bucket from storage", slog.String("error", err.Error()))
		}
	}()
	logger.Info("bucket deleted")
	return nil, nil
}

func (bs *BucketService) ListAllBuckets(ctx context.Context, req *pb.ListAllBucketsRequest) (*pb.ListAllBucketsResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("listing buckets request with invalid uid")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	buckets, err := bs.bucketsRepo.ListAllBuckets(ownerID)
	if err != nil {
		logger.Error("error listing buckets", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, err.Error())
	}
	result := make([]*pb.Bucket, 0, len(buckets))
	for _, b := range buckets {
		result = append(result, &pb.Bucket{
			Id:        b.ID.String(),
			Name:      b.Name,
			OwnerId:   b.OwnerID.String(),
			CreatedAt: b.CreatedAt.String(),
		})
	}
	logger.Info("successfully listed buckets")
	return &pb.ListAllBucketsResponse{
		Buckets: result,
	}, nil
}

func (bs *BucketService) CheckExistBucket(ctx context.Context, req *pb.CheckExistBucketRequest) (*pb.CheckExistBucketResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("check exist bucket request with invalid uid")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	exist, err := bs.bucketsRepo.CheckExist(ownerID, req.Name)
	if err != nil {
		logger.Error("check bucket repository error", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, err.Error())
	}
	if exist {
		logger.Info("bucket found")
		return &pb.CheckExistBucketResponse{}, nil
	}
	return nil, status.Error(codes.NotFound, "bucket doesn't exist")
}
