package services

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/lim-bo/barn/pkg/models"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type UsersByAccessKeyRepository interface {
	GetByAccessKey(accessKey string) (*models.User, error)
}

type SignatureValidator struct {
	usersRepo UsersByAccessKeyRepository
}

func NewSignatureValidator(repo UsersByAccessKeyRepository) *SignatureValidator {
	return &SignatureValidator{
		usersRepo: repo,
	}
}

const (
	headerAccessKey  = "x-access-key"
	headerSecret     = "x-plain-secret"
	headerSignature  = "x-signature"
	contextUserIDKey = "Owner-ID"
)

func (sv *SignatureValidator) AuthInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	reqID := ctx.Value("Request-ID").(string)
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.Unauthenticated, "lack of headers")
	}
	slog.Debug("headers", slog.Any("access_key", md.Get(headerAccessKey)), slog.Any("secret", md.Get(headerSecret)), slog.Any("sign", md.Get(headerSignature)))
	if len(md.Get(headerAccessKey)) != 1 || len(md.Get(headerSecret)) != 1 || len(md.Get(headerSignature)) != 1 {
		slog.Error("auth failed: invalid headers", slog.String("req_id", reqID))
		return nil, status.Error(codes.Unauthenticated, "invalid auth headers")
	}
	accessKey := md.Get(headerAccessKey)[0]
	secretKey := md.Get(headerSecret)[0]
	signature := md.Get(headerSignature)[0]

	user, err := sv.usersRepo.GetByAccessKey(accessKey)
	if err != nil {
		slog.Error("auth failed: user not fount", slog.String("req_id", reqID))
		return nil, status.Error(codes.NotFound, "no user with such key")
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.SecretKeyHash), []byte(secretKey)); err != nil {
		slog.Error("auth failed: wrong secret key", slog.String("req_id", reqID))
		return nil, status.Error(codes.PermissionDenied, "wrong secret")
	}

	if !isFresh(md.Get("x-timestamp")[0]) {
		slog.Error("auth failed: request too old", slog.String("req_id", reqID))
		return nil, status.Error(codes.PermissionDenied, "request time expired")
	}

	expectedSignature := getHMACSignature(ctx, secretKey)
	if !hmac.Equal([]byte(signature), []byte(expectedSignature)) {
		slog.Error("auth failed: unmatched signature", slog.String("req_id", reqID))
		return nil, status.Error(codes.PermissionDenied, "invalid signature")
	}
	ctx = context.WithValue(ctx, contextUserIDKey, user.ID.String())
	return handler(ctx, req)
}

func getHMACSignature(ctx context.Context, secretKey string) string {
	md, _ := metadata.FromIncomingContext(ctx)
	method := getHeader(md, "x-method")
	path := getHeader(md, "x-path")
	query := getHeader(md, "x-query")
	timestamp := getHeader(md, "x-timestamp")
	slog.Debug("signature values", slog.String("method", method), slog.String("path", path), slog.String("query", query), slog.String("ts", timestamp))
	var b strings.Builder
	b.WriteString(method + "\n")
	b.WriteString(path + "\n")
	if query != "" {
		b.WriteString("?" + query + "\n")
	}
	b.WriteString(timestamp)

	h := hmac.New(sha256.New, []byte(secretKey))
	io.WriteString(h, b.String())
	return hex.EncodeToString(h.Sum(nil))
}

func getHeader(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

func isFresh(ts string) bool {
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return false
	}
	diff := time.Since(t)
	return diff < 5*time.Minute && diff > -5*time.Minute
}
