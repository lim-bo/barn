package errvalues

import "errors"

var (
	ErrNoBucket      = errors.New("no bucket with such name")
	ErrNoUser        = errors.New("bucket with unexist owner")
	ErrExistBucket   = errors.New("bucket already exist")
	ErrInvalidUID    = errors.New("given ownerID is invalid")
	ErrInvalidBucket = errors.New("invalid bucket name")
	ErrRepository    = errors.New("repository error")
	ErrUnexistUser   = errors.New("user not found")
	ErrUserExists    = errors.New("user already exist")
	ErrUnexistObject = errors.New("object with such key not found")
)
