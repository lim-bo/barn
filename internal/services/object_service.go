package services

import (
	"bytes"
	"context"
	"errors"
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
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	ObjectSizeHeader         = "content-lenght"
	ObjectETagHeader         = "etag"
	ObjectLastModifiedHeader = "last-modified"

	MultipartStatusInited    = "inited"
	MultipartStatusCompleted = "completed"
	MultipartStatusAborted   = "aborted"
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

type MultipartRepository interface {
	CreateUpload(upload *models.Upload) error
	ChangeUploadState(uploadID uuid.UUID, state string) error
	AddUploadPart(uploadID uuid.UUID, part *models.UploadPart) error
	ListUploads(ownerID uuid.UUID, bucket string) ([]*models.Upload, error)
	ListParts(uploadID uuid.UUID) ([]*models.UploadPart, error)
}

type ObjectService struct {
	objRepo  ObjectRepository
	multRepo MultipartRepository

	objStorage  storage.ObjectStorage
	multStorage storage.MultipartStorage
	pb.UnimplementedObjectServiceServer
}

type ObjectServiceConfig struct {
	ObjRepo          ObjectRepository
	ObjStorage       storage.ObjectStorage
	MultipartRepo    MultipartRepository
	MultipartStorage storage.MultipartStorage
}

func NewObjectService(cfg ObjectServiceConfig) *ObjectService {
	return &ObjectService{
		objRepo:     cfg.ObjRepo,
		objStorage:  cfg.ObjStorage,
		multRepo:    cfg.MultipartRepo,
		multStorage: cfg.MultipartStorage,
	}
}

func (os *ObjectService) LoadObject(ctx context.Context, req *pb.LoadObjectRequest) (*pb.LoadObjectResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("incoming unauthorized request")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	rawData := bytes.Clone(req.Body.Data)
	fileData := bytes.NewReader(rawData)
	var etag string
	slog.Debug("values", slog.String("key", req.Key), slog.String("bucket", req.Bucket), slog.Any("data", rawData))
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		err = os.objStorage.SaveObject(context.Background(), ownerID.String()+"_"+req.Bucket, req.Key, fileData)
	}()
	go func() {
		defer wg.Done()
		etag = GenerateEtag(rawData)
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

func (os *ObjectService) GetObject(ctx context.Context, req *pb.GetObjectRequest) (*httpbody.HttpBody, error) {
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
	logger.Info("object provided")
	md := metadata.Pairs("Content-Type", "application/octet-stream")
	err = grpc.SendHeader(ctx, md)
	if err != nil {
		logger.Error("sending header error", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to set headers")
	}
	return &httpbody.HttpBody{
		ContentType: "application/octet-stream",
		Data:        data,
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

func (os *ObjectService) InitMultipart(ctx context.Context, req *pb.InitMultipartRequest) (*pb.InitMultipartResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("incoming unauthorized request")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	uploadID, err := os.multStorage.InitMultipartUpload(ctx)
	if err != nil {
		logger.Error("failed to init upload in filesystem", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to create upload in filesystem")
	}
	upload := models.Upload{
		ID:      uploadID,
		Bucket:  req.Bucket,
		Key:     req.Key,
		OwnerID: ownerID,
	}
	err = os.multRepo.CreateUpload(&upload)
	if err != nil {
		logger.Error("failed to save new upload's metadata", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to save upload metadata")
	}
	return &pb.InitMultipartResponse{
		UploadId: uploadID.String(),
	}, nil
}

func (os *ObjectService) UploadPart(ctx context.Context, req *pb.UploadPartRequest) (*pb.UploadPartResponse, error) {
	logger := LoggerFromContext(ctx)
	uploadID, err := uuid.Parse(req.UploadId)
	if err != nil {
		logger.Error("upload part request with invalid uploadID")
		return nil, status.Error(codes.InvalidArgument, "invalid upload ID")
	}
	etag, err := os.multStorage.UploadPart(ctx, uploadID, int(req.PartNumber), bytes.NewReader(req.Body.Data))
	if err != nil {
		logger.Error("error while saving upload part locally", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to upload part in filesystem")
	}
	err = os.multRepo.AddUploadPart(uploadID, &models.UploadPart{
		Etag:   etag,
		Number: int(req.PartNumber),
		Size:   int64(len(req.Body.Data)),
	})
	if err != nil {
		switch {
		case errors.Is(err, errvalues.ErrUnexistUpload):
			logger.Error("uploading part to unexist upload")
			return nil, status.Error(codes.InvalidArgument, "upload doesn't exist")
		case errors.Is(err, errvalues.ErrUploadAborted):
			logger.Error("uploading part to aborted upload")
			return nil, status.Error(codes.Aborted, "upload was already aborted")
		case errors.Is(err, errvalues.ErrUploadCompleted):
			logger.Error("uploading part to completed upload")
			return nil, status.Error(codes.Aborted, "upload was already completed")
		case errors.Is(err, errvalues.ErrRepeatedUploadPart):
			logger.Error("uploading already existed part")
			return nil, status.Error(codes.AlreadyExists, "this part already exists")
		default:
			logger.Error("error while saving part metadata", slog.String("error", err.Error()))
			return nil, status.Error(codes.Internal, "failed to upload part metadata")
		}
	}
	return &pb.UploadPartResponse{
		Etag: etag,
	}, nil
}
func (os *ObjectService) CompleteMultipart(ctx context.Context, req *pb.CompleteMultipartRequest) (*pb.CompleteMultipartResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("incoming unauthorized request")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	uploadID, err := uuid.Parse(req.UploadId)
	if err != nil {
		logger.Error("upload part request with invalid uploadID")
		return nil, status.Error(codes.InvalidArgument, "invalid upload ID")
	}
	parts := make([]storage.UploadedPart, 0, len(req.Parts))
	for _, p := range req.Parts {
		parts = append(parts, storage.UploadedPart{
			PartNumber: int(p.PartNumber),
			ETag:       p.Etag,
		})
	}
	etag, err := os.multStorage.CompleteUpload(ctx, storage.UploadMetadata{
		ID:     uploadID,
		Bucket: ownerID.String() + "_" + req.Bucket,
		Key:    req.Key,
		Parts:  parts,
	})
	if err != nil {
		logger.Error("error while merging upload parts", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to merge files")
	}
	err = os.multRepo.ChangeUploadState(uploadID, MultipartStatusCompleted)
	if err != nil {
		if errors.Is(err, errvalues.ErrUploadAborted) {
			logger.Error("attempt to complete aborted upload")
			return nil, status.Error(codes.Aborted, "upload was already aborted")
		}
		if errors.Is(err, errvalues.ErrUploadCompleted) {
			logger.Error("upload was already completed")
			return nil, status.Error(codes.Aborted, "upload was already completed")
		}
		slog.Error("error while completing upload", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to change upload state")
	}
	return &pb.CompleteMultipartResponse{
		Etag:      etag,
		PartCount: int32(len(parts)),
	}, nil
}
func (os *ObjectService) AbortMultipart(ctx context.Context, req *pb.AbortMultipartRequest) (*pb.AbortMultipartResponse, error) {
	logger := LoggerFromContext(ctx)
	uploadID, err := uuid.Parse(req.UploadId)
	if err != nil {
		logger.Error("upload part request with invalid uploadID")
		return nil, status.Error(codes.InvalidArgument, "invalid upload ID")
	}
	err = os.multStorage.AbortUpload(ctx, uploadID)
	if err != nil {
		logger.Error("error trying delete upload in filesystem", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to abort upload in filesystem")
	}
	err = os.multRepo.ChangeUploadState(uploadID, MultipartStatusAborted)
	if err != nil {
		if errors.Is(err, errvalues.ErrUploadAborted) {
			logger.Error("upload was aborted already")
			return nil, status.Error(codes.Aborted, "upload was already aborted")
		}
		if errors.Is(err, errvalues.ErrUploadCompleted) {
			logger.Error("upload was completed")
			return nil, status.Error(codes.Aborted, "upload was already completed")
		}
		slog.Error("error while aborting upload", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to change upload state")
	}
	return &pb.AbortMultipartResponse{
		Aborted: true,
	}, nil
}

func (os *ObjectService) ListMultipartUploads(ctx context.Context, req *pb.ListMultipartUploadsRequest) (*pb.ListMultipartUploadsResponse, error) {
	logger := LoggerFromContext(ctx)
	ownerID, err := uuid.Parse(ctx.Value("Owner-ID").(string))
	if err != nil {
		logger.Error("incoming unauthorized request")
		return nil, status.Error(codes.Unauthenticated, errvalues.ErrInvalidUID.Error())
	}
	uploads, err := os.multRepo.ListUploads(ownerID, req.Bucket)
	if err != nil {
		logger.Error("error while fetching uploads list", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to get uploads list")
	}
	resultUploads := make([]*pb.Upload, 0, len(uploads))
	for _, u := range uploads {
		resultUploads = append(resultUploads, &pb.Upload{
			Key:       u.Key,
			UploadId:  u.ID.String(),
			CreatedAt: u.CreatedAt.Format(time.RFC3339),
		})
	}
	return &pb.ListMultipartUploadsResponse{
		Bucket:  req.Bucket,
		Uploads: resultUploads,
	}, nil
}
func (os *ObjectService) ListParts(ctx context.Context, req *pb.ListPartsRequest) (*pb.ListPartsResponse, error) {
	logger := LoggerFromContext(ctx)
	uploadID, err := uuid.Parse(req.UploadId)
	if err != nil {
		logger.Error("upload part request with invalid uploadID")
		return nil, status.Error(codes.InvalidArgument, "invalid upload ID")
	}
	parts, err := os.multRepo.ListParts(uploadID)
	if err != nil {
		logger.Error("error while fetching upload parts list", slog.String("error", err.Error()))
		return nil, status.Error(codes.Internal, "failed to get parts list")
	}
	resultParts := make([]*pb.UploadPart, 0, len(parts))
	for _, p := range parts {
		resultParts = append(resultParts, &pb.UploadPart{
			PartNumber: int32(p.Number),
			Etag:       p.Etag,
			Size:       p.Size,
			CreatedAt:  p.CreatedAt.Format(time.RFC3339),
		})
	}
	return &pb.ListPartsResponse{
		Bucket:   req.Bucket,
		Key:      req.Key,
		UploadId: uploadID.String(),
		Parts:    resultParts,
	}, nil
}
