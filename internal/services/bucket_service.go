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
	br            BucketsRepositoryI
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
		br:            br,
		storageEngine: bucketStorage,
	}
}

func (bs *BucketService) CreateBucket(ctx context.Context, req *pb.CreateBucketRequest) (*pb.CreateBucketResponse, error) {
	reqID := ctx.Value("Request-ID").(string)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		slog.Error("create bucket request with invalid uid", slog.String("req_id", reqID))
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	bucket, err := bs.br.CreateBucket(ownerID, req.Name)
	if err != nil {
		slog.Error("error creating bucket", slog.String("error", err.Error()), slog.String("req_id", reqID), slog.String("uid", ownerID.String()))
		return nil, status.Error(codes.Internal, err.Error())
	}
	go func() {
		err := bs.storageEngine.CreateBucket(context.Background(), req.Name)
		if err != nil {
			slog.Error("error created bucket in storage", slog.String("error", err.Error()), slog.String("req_id", reqID),
				slog.String("uid", ownerID.String()))
		}
	}()
	slog.Info("successfully created bucket", slog.String("req_id", reqID), slog.String("uid", ownerID.String()))
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
	reqID := ctx.Value("Request-ID").(string)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	err = bs.br.DeleteBucket(ownerID, req.Name)
	if err != nil {
		slog.Error("error deleting bucket", slog.String("error", err.Error()), slog.String("req_id", reqID), slog.String("uid", ownerID.String()))
		return nil, status.Error(codes.Internal, err.Error())
	}
	go func() {
		err := bs.storageEngine.DeleteBucket(context.Background(), req.Name)
		if err != nil {
			slog.Error("error deleting bucket from storage", slog.String("error", err.Error()), slog.String("req_id", reqID), slog.String("uid", ownerID.String()))
		}
	}()
	slog.Info("bucket deleted", slog.String("req_id", reqID), slog.String("uid", ownerID.String()))
	return nil, nil
}

func (bs *BucketService) ListAllBuckets(ctx context.Context, req *pb.ListAllBucketsRequest) (*pb.ListAllBucketsResponse, error) {
	reqID := ctx.Value("Request-ID").(string)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	buckets, err := bs.br.ListAllBuckets(ownerID)
	if err != nil {
		slog.Error("error listing buckets", slog.String("error", err.Error()), slog.String("req_id", reqID), slog.String("uid", ownerID.String()))
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
	return &pb.ListAllBucketsResponse{
		Buckets: result,
	}, nil
}

func (bs *BucketService) CheckExistBucket(ctx context.Context, req *pb.CheckExistBucketRequest) (*pb.CheckExistBucketResponse, error) {
	reqID := ctx.Value("Request-ID").(string)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	exist, err := bs.br.CheckExist(ownerID, req.Name)
	if err != nil {
		slog.Error("check bucket repository error", slog.String("error", err.Error()), slog.String("req_id", reqID), slog.String("uid", ownerID.String()))
		return nil, status.Error(codes.Internal, err.Error())
	}
	if exist {
		return &pb.CheckExistBucketResponse{}, nil
	}
	return nil, status.Error(codes.NotFound, "bucket doesn't exist")
}
