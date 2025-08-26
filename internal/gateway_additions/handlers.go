package gateway

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/lim-bo/barn/internal/services/pb"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/metadata"
)

type ExtendedGWMux struct {
	objClient pb.ObjectServiceClient
	gwMux     http.Handler
}

func NewExtendedGWMux(cli pb.ObjectServiceClient, gwMux http.Handler) *ExtendedGWMux {
	return &ExtendedGWMux{
		objClient: cli,
		gwMux:     gwMux,
	}
}

func (m *ExtendedGWMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pathSegs := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case r.Method == http.MethodPost && len(pathSegs) == 2 && q.Has("uploads"):
		m.initMultipartHandler(w, r)
	case r.Method == http.MethodPut && len(pathSegs) == 2 && q.Has("partNumber") && q.Has("uploadId"):
		m.uploadPartHandler(w, r)
	case r.Method == http.MethodPut && len(pathSegs) == 2 && !q.Has("partNumber") && !q.Has("uploadId"):
		m.putObjectHandler(w, r)
	case r.Method == http.MethodPost && len(pathSegs) == 2 && q.Has("uploadId"):
		m.completeMultipart(w, r)
	case r.Method == http.MethodGet && len(pathSegs) == 2 && q.Has("uploadId"):
		m.listUploadParts(w, r)
	case r.Method == http.MethodGet && len(pathSegs) == 1 && q.Has("uploads"):
		m.listUploads(w, r)
	default:
		m.gwMux.ServeHTTP(w, r)
	}
}

func (m *ExtendedGWMux) initMultipartHandler(w http.ResponseWriter, r *http.Request) {
	pathValues := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	bucket := pathValues[0]
	key := pathValues[1]
	if bucket == "" || key == "" {
		http.Error(w, "invalid path bucket and key values", http.StatusBadRequest)
		return
	}
	md := metadata.MD{
		"x-plain-secret": []string{r.Header.Get("Grpc-Metadata-X-Plain-Secret")},
		"x-access-key":   []string{r.Header.Get("Grpc-Metadata-X-Access-Key")},
		"x-signature":    []string{r.Header.Get("Grpc-Metadata-X-Signature")},
		"x-method":       []string{r.Header.Get("Grpc-Metadata-X-Method")},
		"x-path":         []string{r.Header.Get("Grpc-Metadata-X-Path")},
		"x-query":        []string{r.Header.Get("Grpc-Metadata-X-Query")},
		"x-timestamp":    []string{r.Header.Get("Grpc-Metadata-X-Timestamp")},
	}
	ctx := metadata.NewOutgoingContext(r.Context(), md)
	resp, err := m.objClient.InitMultipart(ctx, &pb.InitMultipartRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		http.Error(w, "error creating multipart upload", http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"upload_id": resp.UploadId,
	})
}

func (m *ExtendedGWMux) completeMultipart(w http.ResponseWriter, r *http.Request) {
	pathValues := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	bucket := pathValues[0]
	key := pathValues[1]
	if bucket == "" || key == "" {
		http.Error(w, "invalid path bucket and key values", http.StatusBadRequest)
		slog.Error("incoming request with invalid bucket and key path values")
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	var CompletedPartRequest struct {
		Parts []struct {
			PartNumber int    `json:"part_number"`
			Etag       string `json:"etag"`
		} `jsob:"parts"`
	}
	err := json.NewDecoder(r.Body).Decode(&CompletedPartRequest)
	if err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		slog.Error("complete multipart request with invalid body data", slog.String("error", err.Error()))
		return
	}
	defer r.Body.Close()
	md := metadata.MD{
		"x-plain-secret": []string{r.Header.Get("Grpc-Metadata-X-Plain-Secret")},
		"x-access-key":   []string{r.Header.Get("Grpc-Metadata-X-Access-Key")},
		"x-signature":    []string{r.Header.Get("Grpc-Metadata-X-Signature")},
		"x-method":       []string{r.Header.Get("Grpc-Metadata-X-Method")},
		"x-path":         []string{r.Header.Get("Grpc-Metadata-X-Path")},
		"x-query":        []string{r.Header.Get("Grpc-Metadata-X-Query")},
		"x-timestamp":    []string{r.Header.Get("Grpc-Metadata-X-Timestamp")},
	}
	ctx := metadata.NewOutgoingContext(r.Context(), md)
	parts := make([]*pb.CompletedPart, 0, len(CompletedPartRequest.Parts))
	for _, p := range CompletedPartRequest.Parts {
		parts = append(parts, &pb.CompletedPart{
			PartNumber: int32(p.PartNumber),
			Etag:       p.Etag,
		})
	}
	resp, err := m.objClient.CompleteMultipart(ctx, &pb.CompleteMultipartRequest{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
		Parts:    parts,
	})
	if err != nil {
		http.Error(w, "error compliting multipart: "+err.Error(), http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"count": int(resp.PartCount),
		"etag":  resp.Etag,
	})
}

func (m *ExtendedGWMux) listUploadParts(w http.ResponseWriter, r *http.Request) {
	pathValues := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	bucket := pathValues[0]
	key := pathValues[1]
	if bucket == "" || key == "" {
		http.Error(w, "invalid path bucket and key values", http.StatusBadRequest)
		slog.Error("incoming request with invalid bucket and key path values")
		return
	}
	uploadID := r.URL.Query().Get("uploadId")
	md := metadata.MD{
		"x-plain-secret": []string{r.Header.Get("Grpc-Metadata-X-Plain-Secret")},
		"x-access-key":   []string{r.Header.Get("Grpc-Metadata-X-Access-Key")},
		"x-signature":    []string{r.Header.Get("Grpc-Metadata-X-Signature")},
		"x-method":       []string{r.Header.Get("Grpc-Metadata-X-Method")},
		"x-path":         []string{r.Header.Get("Grpc-Metadata-X-Path")},
		"x-query":        []string{r.Header.Get("Grpc-Metadata-X-Query")},
		"x-timestamp":    []string{r.Header.Get("Grpc-Metadata-X-Timestamp")},
	}
	ctx := metadata.NewOutgoingContext(r.Context(), md)
	resp, err := m.objClient.ListParts(ctx, &pb.ListPartsRequest{
		Bucket:   bucket,
		Key:      key,
		UploadId: uploadID,
	})
	if err != nil {
		http.Error(w, "error getting parts: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var PartsListResponse struct {
		Bucket   string `json:"bucket"`
		Key      string `json:"key"`
		UploadID string `json:"upload_id"`
		Parts    []struct {
			PartNumber int    `json:"part_number"`
			Etag       string `json:"etag"`
			Size       int64  `json:"size"`
			CreatedAt  string `json:"created_at"`
		}
	}
	for _, p := range resp.Parts {
		PartsListResponse.Parts = append(PartsListResponse.Parts, struct {
			PartNumber int    "json:\"part_number\""
			Etag       string "json:\"etag\""
			Size       int64  "json:\"size\""
			CreatedAt  string "json:\"created_at\""
		}{
			Size:       p.Size,
			Etag:       p.Etag,
			PartNumber: int(p.PartNumber),
			CreatedAt:  p.CreatedAt,
		})
	}
	err = json.NewEncoder(w).Encode(PartsListResponse)
	if err != nil {
		http.Error(w, "error providing results", http.StatusInternalServerError)
		slog.Error("error marshalling results", slog.String("error", err.Error()))
		return
	}
}

func (m *ExtendedGWMux) listUploads(w http.ResponseWriter, r *http.Request) {
	pathValues := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	bucket := pathValues[0]
	if bucket == "" {
		http.Error(w, "invalid path bucket value", http.StatusBadRequest)
		slog.Error("incoming request with invalid bucket path value")
		return
	}
	md := metadata.MD{
		"x-plain-secret": []string{r.Header.Get("Grpc-Metadata-X-Plain-Secret")},
		"x-access-key":   []string{r.Header.Get("Grpc-Metadata-X-Access-Key")},
		"x-signature":    []string{r.Header.Get("Grpc-Metadata-X-Signature")},
		"x-method":       []string{r.Header.Get("Grpc-Metadata-X-Method")},
		"x-path":         []string{r.Header.Get("Grpc-Metadata-X-Path")},
		"x-query":        []string{r.Header.Get("Grpc-Metadata-X-Query")},
		"x-timestamp":    []string{r.Header.Get("Grpc-Metadata-X-Timestamp")},
	}
	ctx := metadata.NewOutgoingContext(r.Context(), md)
	resp, err := m.objClient.ListMultipartUploads(ctx, &pb.ListMultipartUploadsRequest{
		Bucket: bucket,
	})
	if err != nil {
		http.Error(w, "error getting uploads: "+err.Error(), http.StatusInternalServerError)
		return
	}
	var ListUploadsResponse struct {
		Bucket  string `json:"bucket"`
		Uploads []struct {
			Key       string `json:"key"`
			CreatedAt string `json:"created_at"`
			UploadID  string `json:"upload_id"`
		}
	}
	for _, u := range resp.Uploads {
		ListUploadsResponse.Uploads = append(ListUploadsResponse.Uploads, struct {
			Key       string "json:\"key\""
			CreatedAt string "json:\"created_at\""
			UploadID  string "json:\"upload_id\""
		}{
			Key:       u.Key,
			CreatedAt: u.CreatedAt,
			UploadID:  u.UploadId,
		})
	}
	err = json.NewEncoder(w).Encode(ListUploadsResponse)
	if err != nil {
		http.Error(w, "error providing results", http.StatusInternalServerError)
		slog.Error("error marshalling results", slog.String("error", err.Error()))
		return
	}
}

func (m *ExtendedGWMux) uploadPartHandler(w http.ResponseWriter, r *http.Request) {
	partNumber, err := strconv.ParseInt(r.URL.Query().Get("partNumber"), 10, 32)
	if err != nil {
		http.Error(w, "invalid query param: part_number", http.StatusBadRequest)
		slog.Error("request for uploading part with invalid part number")
		return
	}
	uploadID, err := uuid.Parse(r.URL.Query().Get("uploadId"))
	if err != nil {
		http.Error(w, "invalid query param: upload_id", http.StatusBadRequest)
		slog.Error("request for uploading part with invalid uid")
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error accepting body", http.StatusBadRequest)
		slog.Error("error ")
		return
	}
	md := metadata.MD{
		"x-plain-secret": []string{r.Header.Get("Grpc-Metadata-X-Plain-Secret")},
		"x-access-key":   []string{r.Header.Get("Grpc-Metadata-X-Access-Key")},
		"x-signature":    []string{r.Header.Get("Grpc-Metadata-X-Signature")},
		"x-method":       []string{r.Header.Get("Grpc-Metadata-X-Method")},
		"x-path":         []string{r.Header.Get("Grpc-Metadata-X-Path")},
		"x-query":        []string{r.Header.Get("Grpc-Metadata-X-Query")},
		"x-timestamp":    []string{r.Header.Get("Grpc-Metadata-X-Timestamp")},
	}
	ctx := metadata.NewOutgoingContext(r.Context(), md)
	resp, err := m.objClient.UploadPart(ctx, &pb.UploadPartRequest{
		Bucket:     r.PathValue("bucket"),
		Key:        r.PathValue("key"),
		PartNumber: int32(partNumber),
		UploadId:   uploadID.String(),
		Body: &httpbody.HttpBody{
			ContentType: "application/octet-stream",
			Data:        data,
		},
	})
	if err != nil {
		http.Error(w, "failed to add upload part", http.StatusInternalServerError)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"etag": resp.Etag,
	})
}

func (m *ExtendedGWMux) putObjectHandler(w http.ResponseWriter, r *http.Request) {
	pathValues := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	bucket := pathValues[0]
	key := pathValues[1]
	if bucket == "" || key == "" {
		http.Error(w, "invalid path bucket and key values", http.StatusBadRequest)
		slog.Error("incoming request with invalid bucket and key path values")
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "error accepting body", http.StatusBadRequest)
		slog.Error("error reading object body", slog.String("error", err.Error()))
		return
	}
	md := metadata.MD{
		"x-plain-secret": []string{r.Header.Get("Grpc-Metadata-X-Plain-Secret")},
		"x-access-key":   []string{r.Header.Get("Grpc-Metadata-X-Access-Key")},
		"x-signature":    []string{r.Header.Get("Grpc-Metadata-X-Signature")},
		"x-method":       []string{r.Header.Get("Grpc-Metadata-X-Method")},
		"x-path":         []string{r.Header.Get("Grpc-Metadata-X-Path")},
		"x-query":        []string{r.Header.Get("Grpc-Metadata-X-Query")},
		"x-timestamp":    []string{r.Header.Get("Grpc-Metadata-X-Timestamp")},
	}
	ctx := metadata.NewOutgoingContext(r.Context(), md)
	resp, err := m.objClient.LoadObject(ctx, &pb.LoadObjectRequest{
		Bucket: bucket,
		Key:    key,
		Body: &httpbody.HttpBody{
			ContentType: "application/octet-stream",
			Data:        data,
		},
	})
	if err != nil {
		http.Error(w, "failed to save object", http.StatusBadGateway)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"etag": resp.Etag,
	})

}
