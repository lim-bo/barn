package services

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func RequestIDInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	reqID := uuid.New()
	ctx = context.WithValue(ctx, "Request-ID", reqID.String())
	slog.Info("incoming request", slog.String("req_id", reqID.String()))
	return handler(ctx, req)
}

func AuthInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	reqID := ctx.Value("Request-ID").(string)
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		slog.Error("request with no headers", slog.String("req_id", reqID))
		return nil, status.Error(codes.Unauthenticated, "missing headers")
	}
	authHeader := md.Get("authorization")
	if len(authHeader) == 0 {
		slog.Error("request missing auth header", slog.String("req_id", reqID))
		return nil, status.Error(codes.Unauthenticated, "missing authorization header")
	}
	ctx = context.WithValue(ctx, "Owner-ID", authHeader[0])
	return handler(ctx, req)
}
