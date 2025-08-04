package services

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/lim-bo/barn/internal/services/pb"
	"github.com/lim-bo/barn/pkg/models"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type UsersRepositoryI interface {
	Create(user *models.User) error
	GetByUsername(name string) (*models.User, error)
	UpdateKeys(uid uuid.UUID, accessKey string, secretKeyHash string) error
}

type AuthService struct {
	usersRepo UsersRepositoryI

	pb.UnimplementedAuthServiceServer
}

func NewAuthService(ur UsersRepositoryI) *AuthService {
	return &AuthService{
		usersRepo: ur,
	}
}

func (as *AuthService) RegisterWithKeys(ctx context.Context, req *emptypb.Empty) (*pb.RegisterResponse, error) {
	reqID := ctx.Value("Request-ID").(string)
	accessKey := GenerateAccessKey()
	secretKey := GenerateSecretKey()
	user := models.User{
		AccessKey:     accessKey,
		SecretKeyHash: HashKey(secretKey),
		Status:        models.StatusActive,
	}
	err := as.usersRepo.Create(&user)
	if err != nil {
		slog.Error("error creating new user", slog.String("error", err.Error()), slog.String("req_id", reqID))
		return nil, status.Error(codes.Unauthenticated, "creating user error")
	}
	slog.Info("new user registered with keys", slog.String("req_id", reqID))
	return &pb.RegisterResponse{
		Keys: &pb.Keys{
			AccessKey: accessKey,
			SecretKey: secretKey,
		},
	}, nil
}

func (as *AuthService) LoginWithPassword(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	reqID := ctx.Value("Request-ID").(string)
	user, err := as.usersRepo.GetByUsername(req.Credentials.Username)
	if err != nil {
		slog.Error("error getting user by name", slog.String("error", err.Error()), slog.String("req_id", reqID))
		return nil, status.Error(codes.NotFound, "getting user error")
	}
	if user.PasswordHash == nil {
		slog.Error("login request for user with no password", slog.String("req_id", reqID))
		return nil, status.Error(codes.PermissionDenied, "user has no password")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(*user.PasswordHash), []byte(req.Credentials.Password)); err != nil {
		return nil, status.Error(codes.Unauthenticated, "wrong password")
	}
	accessKey := GenerateAccessKey()
	secretKey := GenerateSecretKey()
	as.usersRepo.UpdateKeys(user.ID, accessKey, HashKey(secretKey))
	return &pb.LoginResponse{
		Keys: &pb.Keys{
			AccessKey: accessKey,
			SecretKey: secretKey,
		},
	}, nil
}

func (as *AuthService) RegisterWithPassword(ctx context.Context, req *pb.RegisterWithPasswordRequest) (*pb.RegisterResponse, error) {
	reqID := ctx.Value("Request-ID").(string)
	accessKey := GenerateAccessKey()
	secretKey := GenerateSecretKey()
	passHash := HashKey(req.Credentials.Password)
	user := models.User{
		AccessKey:     accessKey,
		SecretKeyHash: HashKey(secretKey),
		Status:        models.StatusActive,
		Username:      &req.Credentials.Username,
		PasswordHash:  &passHash,
	}
	err := as.usersRepo.Create(&user)
	if err != nil {
		slog.Error("error creating new user", slog.String("error", err.Error()), slog.String("req_id", reqID))
		return nil, status.Error(codes.Unauthenticated, "creating user error")
	}
	slog.Info("new user registered with creds", slog.String("req_id", reqID))
	return &pb.RegisterResponse{
		Keys: &pb.Keys{
			AccessKey: accessKey,
			SecretKey: secretKey,
		},
	}, nil
}
