package storage

import (
	"crypto/md5"
	"fmt"
)

func generateEtag(data []byte) string {
	return fmt.Sprintf("\"%x\"", md5.Sum(data))
}
