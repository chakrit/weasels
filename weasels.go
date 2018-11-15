package weasels

import (
	"github.com/dgraph-io/badger"
)

type Weasels interface {
	Badger() *badger.DB

	Read(key []byte) ([]byte, error)
	ReadAll(keys [][]byte) ([][]byte, error)
	Write(key, value []byte) error
	WriteAll(keys [][]byte, values [][]byte) error
	Delete(key []byte) error
	DeleteAll(keys [][]byte) error

	Scan(keyPrefix []byte) ([][]byte, error)
	ReadScan(keyPrefix []byte) ([][]byte, [][]byte, error)
	DeleteScan(keyPrefix []byte) ([][]byte, error)
}

func Open(dir string) (Weasels, error) {
	opts := badger.DefaultOptions
	opts.Dir = dir
	opts.ValueDir = dir

	if db, err := badger.Open(opts); err != nil {
		return nil, &Error{
			Code:    ErrCodeBadger,
			Message: "failed to open underlying database",
			Cause:   err,
		}
	} else {
		return &impl{db}, nil
	}
}
