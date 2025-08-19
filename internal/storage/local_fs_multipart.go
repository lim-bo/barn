package storage

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/google/uuid"
)

const (
	uploadsDirName = ".uploads"
)

type MultipartLocalFS struct {
	root string
}

func NewMultipartLocalFS(uploadsRoot string) *MultipartLocalFS {
	return &MultipartLocalFS{
		root: uploadsRoot,
	}
}

func (mpfs *MultipartLocalFS) InitMultipartUpload(ctx context.Context) (uuid.UUID, error) {
	uploadID := uuid.New()
	path := filepath.Join(mpfs.root, uploadsDirName, uploadID.String())
	err := os.MkdirAll(path, 0755)
	if err != nil {
		return uuid.UUID{}, errors.New("creating upload directory error: " + err.Error())
	}
	return uploadID, nil
}

func (mpfs *MultipartLocalFS) UploadPart(ctx context.Context, uploadID uuid.UUID, partNumber int, data io.Reader) (string, error) {
	path := filepath.Join(mpfs.root, uploadsDirName, uploadID.String(), fmt.Sprintf("%d.part", partNumber))
	file, err := os.Create(path)
	if err != nil {
		return "", errors.New("creating part file error: " + err.Error())
	}
	defer file.Close()
	content, err := io.ReadAll(data)
	if err != nil {
		return "", errors.New("getting part content error: " + err.Error())
	}
	_, err = file.Write(content)
	if err != nil {
		return "", errors.New("writing part content error: " + err.Error())
	}
	return generateEtag(content), nil
}

func (mpfs *MultipartLocalFS) CompleteUpload(ctx context.Context, upload UploadMetadata) (string, error) {
	partsPath := filepath.Join(mpfs.root, uploadsDirName, upload.ID.String())
	resultPath := filepath.Join(mpfs.root, upload.Bucket, upload.Key)
	if err := os.MkdirAll(filepath.Dir(resultPath), 0755); err != nil {
		return "", errors.New("creating multipart upload file destination: " + err.Error())
	}
	out, err := os.Create(resultPath)
	if err != nil {
		return "", errors.New("error creating result file: " + err.Error())
	}
	defer out.Close()

	resultEtag, count := getEtagAndCount(checkPart(
		pushParts(upload.Parts),
		partsPath,
		out,
	))
	if err = os.RemoveAll(partsPath); err != nil {
		return "", errors.New("error cleaning uploads dir: " + err.Error())
	}
	if count != len(upload.Parts) {
		return "", errors.New("some parts are missing or haven't been writen")
	}
	return resultEtag, nil
}

func pushParts(parts []UploadedPart) <-chan UploadedPart {
	out := make(chan UploadedPart)
	go func() {
		for _, p := range parts {
			out <- p
		}
		close(out)
	}()
	return out
}

func checkPart(in <-chan UploadedPart, partsDir string, dst io.Writer) <-chan string {
	out := make(chan string)
	go func() {
		for p := range in {
			path := filepath.Join(partsDir, fmt.Sprintf("%d.part", p.PartNumber))
			f, err := os.Open(path)
			if err != nil {
				continue
			}
			ph := md5.New()
			_, err = io.Copy(io.MultiWriter(dst, ph), f)
			if err != nil {
				continue
			}
			f.Close()
			etag := "\"" + hex.EncodeToString(ph.Sum(nil)) + "\""
			if p.ETag != etag {
				continue
			}
			out <- etag
		}
		close(out)
	}()
	return out
}

func getEtagAndCount(in <-chan string) (string, int) {
	cnt := 0
	var resultEtag string
	for etag := range in {
		resultEtag += etag
		cnt++
	}
	return fmt.Sprintf("%s-%d", resultEtag, cnt), cnt
}

func (mpfs *MultipartLocalFS) AbortUpload(ctx context.Context, uploadID uuid.UUID) error {
	path := filepath.Join(mpfs.root, uploadsDirName, uploadID.String())
	err := os.RemoveAll(path)
	if err != nil {
		return errors.New("removing upload error: " + err.Error())
	}
	return nil
}
