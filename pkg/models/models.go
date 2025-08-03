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

type User struct {
	ID            uuid.UUID
	AccessKey     string
	SecretKeyHash string
	Username      *string
	PasswordHash  *string
	Status        string
	CreatedAt     time.Time
}
