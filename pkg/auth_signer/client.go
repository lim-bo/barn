package authsigner

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"strings"
	"time"
)

type Signer struct {
	accessKey string
	secretKey string
}

func New(accessKey string, secretKey string) *Signer {
	return &Signer{
		accessKey: accessKey,
		secretKey: secretKey,
	}
}

func (s *Signer) SignRequest(req *http.Request) error {
	req.Header.Set("Grpc-Metadata-X-Access-Key", s.accessKey)
	req.Header.Set("Grpc-Metadata-X-Plain-Secret", s.secretKey)
	ts := time.Now().Format(time.RFC3339)
	req.Header.Set("Grpc-Metadata-X-Timestamp", ts)

	var signatureBuilder strings.Builder
	signatureBuilder.WriteString(req.Method + "\n")
	req.Header.Set("Grpc-Metadata-X-Method", req.Method)
	signatureBuilder.WriteString(req.URL.Path + "\n")
	req.Header.Set("Grpc-Metadata-X-Path", req.URL.Path)
	if q := req.URL.RawQuery; q != "" {
		signatureBuilder.WriteString("?" + q + "\n")
		req.Header.Set("Grpc-Metadata-X-Query", q)
	}
	signatureBuilder.WriteString(ts)
	h := hmac.New(sha256.New, []byte(s.secretKey))
	io.WriteString(h, signatureBuilder.String())
	signature := hex.EncodeToString(h.Sum(nil))
	req.Header.Set("Grpc-Metadata-X-Signature", signature)
	return nil
}
