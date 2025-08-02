package repos

import "regexp"

var (
	bucketNameRegexp = regexp.MustCompile("^[A-z,a-z,0-9]{1,}$")
)

func validateBucketName(bucket string) bool {
	return bucketNameRegexp.MatchString(bucket)
}
