package main

import (
	"strings"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
)

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
