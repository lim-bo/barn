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
