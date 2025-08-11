package main

import (
	"bytes"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
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
	}
)

func headerMatcher(key string) (string, bool) {
	if allowedHeaders[key] {
		return key, true
	}
	return runtime.DefaultHeaderMatcher(key)
}

type OctetStreamMarshaller struct {
}

func (m *OctetStreamMarshaller) Marshal(v interface{}) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	return nil, errors.New("unsupported type")
}

func (m *OctetStreamMarshaller) ContentType(v interface{}) string {
	return "application/octet-stream"
}

func (m *OctetStreamMarshaller) Unmarshal(data []byte, v interface{}) error {
	if p, ok := v.(*[]byte); ok {
		*p = append((*p)[0:0], data...)
		return nil
	}
	return errors.New("unsupported type")
}

func (m *OctetStreamMarshaller) NewDecoder(r io.Reader) runtime.Decoder {
	return runtime.DecoderFunc(func(v interface{}) error {
		if p, ok := v.(*[]byte); ok {
			buf := new(bytes.Buffer)
			_, err := io.Copy(buf, r)
			if err != nil {
				return err
			}
			*p = buf.Bytes()
			return nil
		}
		return errors.New("unsupported type")
	})
}

func (m *OctetStreamMarshaller) NewEncoder(w io.Writer) runtime.Encoder {
	return runtime.EncoderFunc(func(v interface{}) error {
		if b, ok := v.([]byte); ok {
			_, err := w.Write(b)
			return err
		}
		return errors.New("unsupported type")
	})
}

func (m *OctetStreamMarshaller) ContentTypeFromMessage(v interface{}) (string, error) {
	return mime.TypeByExtension(".bin"), nil
}
