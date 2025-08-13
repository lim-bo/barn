package services

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"

	"golang.org/x/crypto/bcrypt"
)

var (
	loggerContextKey = "logger"
)

func GenerateAccessKey() string {
	return "AKIA" + randString(16)
}
func GenerateSecretKey() string {
	return randString(32)
}

func randString(l int) string {
	b := make([]byte, (l+1)/2)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func HashKey(key string) string {
	hash, _ := bcrypt.GenerateFromPassword([]byte(key), bcrypt.DefaultCost)
	return string(hash)
}

func GenerateEtag(data []byte) string {
	return fmt.Sprintf("\"%x\"", md5.Sum(data))
}

func LoggerFromContext(ctx context.Context) *slog.Logger {
	if logger, ok := ctx.Value(loggerContextKey).(*slog.Logger); ok {
		return logger
	}
	return slog.Default()
}
