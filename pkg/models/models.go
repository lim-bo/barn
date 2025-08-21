package models

import (
	"time"

	"github.com/google/uuid"
)

type Bucket struct {
	ID        uuid.UUID
	Name      string
	OwnerID   uuid.UUID
	CreatedAt time.Time
}

var (
	StatusActive = "active"
)

type User struct {
	ID            uuid.UUID
	AccessKey     string
	SecretKeyHash string
	Username      *string
	PasswordHash  *string
	Status        string
	CreatedAt     time.Time
}

type Object struct {
	ID           uuid.UUID
	BucketID     uuid.UUID
	Key          string
	Size         uint64
	Etag         string
	LastModified time.Time
}

type Upload struct {
	ID        uuid.UUID
	Bucket    string
	Key       string
	OwnerID   uuid.UUID
	CreatedAt time.Time
	Status    string
}

type UploadPart struct {
	Number    int
	Size      int64
	Etag      string
	CreatedAt time.Time
}
