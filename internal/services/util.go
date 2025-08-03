package services

import (
	"crypto/rand"
	"encoding/hex"

	"golang.org/x/crypto/bcrypt"
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
