package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/lim-bo/barn/internal/services/pb"
	"google.golang.org/genproto/googleapis/api/httpbody"
	"google.golang.org/grpc/metadata"
)

func CORSMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD")
		w.Header().Set("Access-Control-Allow-Headers", "X-Access-Key, X-Plain-Secret, X-Signature, X-Timestamp, Content-Type, "+
			"X-Method, X-Path, X-Query")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

var (
	allowedHeaders = map[string]bool{
		"x-access-key":   true,
		"x-plain-secret": true,
		"x-signature":    true,
		"x-timestamp":    true,
		"x-method":       true,
		"x-path":         true,
		"content-type":   true,
		"content-lenght": true,
		"etag":           true,
		"last-modified":  true,
	}
)

func headerMatcher(key string) (string, bool) {
	if allowedHeaders[strings.ToLower(key)] {
		return key, true
	}
	return runtime.DefaultHeaderMatcher(key)
}

func binaryRequestHandler(objClient pb.ObjectServiceClient) func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
	return func(w http.ResponseWriter, r *http.Request, pathParams map[string]string) {
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
		resp, err := objClient.LoadObject(ctx, &pb.LoadObjectRequest{
			Bucket: pathParams["bucket"],
			Key:    pathParams["key"],
			Body: &httpbody.HttpBody{
				ContentType: "application/octet-stream",
				Data:        data,
			},
		})
		if err != nil {
			http.Error(w, "failed to save object", http.StatusBadGateway)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"etag": resp.Etag,
		})
	}

}
