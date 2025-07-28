package errvalues

import "errors"

var (
	ErrNoBucket    = errors.New("no bucket with such name")
	ErrNoUser      = errors.New("bucket with unexist owner")
	ErrExistBucket = errors.New("bucket already exist")
)
