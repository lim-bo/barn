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
	logger := ctx.Value("logger").(*slog.Logger)
	accessKey := GenerateAccessKey()
	secretKey := GenerateSecretKey()
	user := models.User{
		AccessKey:     accessKey,
		SecretKeyHash: HashKey(secretKey),
		Status:        models.StatusActive,
	}
	err := as.usersRepo.Create(&user)
	if err != nil {
		logger.Error("error creating new user", slog.String("error", err.Error()))
		return nil, status.Error(codes.Unauthenticated, "creating user error")
	}
	logger.Info("new user registered with keys")
	return &pb.RegisterResponse{
		Keys: &pb.Keys{
			AccessKey: accessKey,
			SecretKey: secretKey,
		},
	}, nil
}

func (as *AuthService) LoginWithPassword(ctx context.Context, req *pb.LoginRequest) (*pb.LoginResponse, error) {
	logger := ctx.Value("logger").(*slog.Logger)
	user, err := as.usersRepo.GetByUsername(req.Credentials.Username)
	if err != nil {
		logger.Error("error getting user by name", slog.String("error", err.Error()))
		return nil, status.Error(codes.NotFound, "getting user error")
	}
	if user.PasswordHash == nil {
		logger.Error("login request for user with no password")
		return nil, status.Error(codes.PermissionDenied, "user has no password")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(*user.PasswordHash), []byte(req.Credentials.Password)); err != nil {
		logger.Error("login request with incorrect password", slog.String("uid", user.ID.String()))
		return nil, status.Error(codes.Unauthenticated, "wrong password")
	}
	accessKey := GenerateAccessKey()
	secretKey := GenerateSecretKey()
	err = as.usersRepo.UpdateKeys(user.ID, accessKey, HashKey(secretKey))
	if err != nil {
		logger.Error("failed to update user's keys", slog.String("error", err.Error()))
	}
	logger.Info("successful login", slog.String("uid", user.ID.String()))
	return &pb.LoginResponse{
		Keys: &pb.Keys{
			AccessKey: accessKey,
			SecretKey: secretKey,
		},
	}, nil
}

func (as *AuthService) RegisterWithPassword(ctx context.Context, req *pb.RegisterWithPasswordRequest) (*pb.RegisterResponse, error) {
	logger := ctx.Value("logger").(*slog.Logger)
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
		logger.Error("error creating new user", slog.String("error", err.Error()))
		return nil, status.Error(codes.Unauthenticated, "creating user error")
	}
	logger.Info("new user registered with creds")
	return &pb.RegisterResponse{
		Keys: &pb.Keys{
			AccessKey: accessKey,
			SecretKey: secretKey,
		},
	}, nil
}
