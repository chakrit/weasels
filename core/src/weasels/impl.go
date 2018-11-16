package weasels

import (
	"bytes"
	"sort"

	"github.com/dgraph-io/badger"
)

type impl struct {
	db *badger.DB
}

func (i *impl) Badger() *badger.DB {
	return i.db
}

func (i *impl) Read(key []byte) (value []byte, err error) {
	err = i.db.View(func(txn *badger.Txn) error {
		if item, err := txn.Get(key); err != nil {
			return newError(ErrCodeBadger, "failed to get key", nil)
		} else if v, err := item.ValueCopy(value); err != nil {
			return newError(ErrCodeBadger, "failed to get value", nil)
		} else {
			value = v
			return nil
		}
	})
	return
}

func (i *impl) ReadAll(keys [][]byte) (values [][]byte, err error) {
	sort.Sort(BytesSorter(keys))
	values = make([][]byte, len(keys), len(keys))

	err = i.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: true})
		defer it.Close()
		if !it.Valid() {
			return newError(ErrCodeEmpty, "iterator finishes prematurely, empty database?", nil)
		}

		for index, key := range keys {
			it.Seek(key)
			if !it.Valid() {
				return nil
			}

			item := it.Item()
			if bytes.Compare(key, item.Key()) != 0 { // key not found, basically
				values[index] = nil
				continue
			}

			if value, err := item.ValueCopy(values[index]); err != nil {
				return newError(ErrCodeBadger, "failed to get value", nil)
			} else {
				values[index] = value
			}
		}

		return nil
	})

	return
}

func (i *impl) Write(key, value []byte) error {
	return i.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(key, value); err != nil {
			return newError(ErrCodeBadger, "failed to set value", nil)
		} else if err := txn.Commit(); err != nil {
			return newError(ErrCodeCommit, "failed to commit writes", err)
		} else {
			return nil
		}
	})
}

func (i *impl) WriteAll(keys [][]byte, values [][]byte) error {
	if len(keys) != len(values) {
		return newError(ErrCodeArg, "keys and values have differing lengths", nil)
	}

	batch := i.db.NewWriteBatch()
	defer batch.Cancel()

	for index, key := range keys {
		if err := batch.Set(key, values[index], 0); err != nil {
			return newError(ErrCodeBadger, "failure during batch writes", err)
		}
	}

	if err := batch.Flush(); err != nil {
		return newError(ErrCodeCommit, "failed to flush writes", err)
	} else {
		return nil
	}
}

func (i *impl) Delete(key []byte) error {
	return i.db.Update(func(txn *badger.Txn) error {
		if err := txn.Delete(key); err != nil {
			return newError(ErrCodeBadger, "failed to delete key", err)
		} else if err := txn.Commit(); err != nil {
			return newError(ErrCodeCommit, "failed to commit delete", err)
		} else {
			return nil
		}
	})
}

func (i *impl) DeleteAll(keys [][]byte) error {
	return i.db.Update(func(txn *badger.Txn) error {
		for _, key := range keys {
			if err := txn.Delete(key); err != nil {
				return newError(ErrCodeBadger, "failed to delete key", err)
			}
		}

		if err := txn.Commit(); err != nil {
			return newError(ErrCodeCommit, "failed to commit deletes", err)
		} else {
			return nil
		}
	})
}

func (i *impl) Scan(keyPrefix []byte) (keys [][]byte, err error) {
	err = i.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
		defer it.Close()
		if !it.Valid() {
			return newError(ErrCodeEmpty, "iterator finishes prematurely, empty database?", nil)
		}

		for it.Seek(keyPrefix); it.ValidForPrefix(keyPrefix); it.Seek(keyPrefix) {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	})
	return
}

func (i *impl) ReadScan(keyPrefix []byte) (keys [][]byte, values [][]byte, err error) {
	err = i.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: true})
		defer it.Close()
		if !it.Valid() {
			return newError(ErrCodeEmpty, "iterator finishes prematurely, empty database?", nil)
		}

		for it.Seek(keyPrefix); it.ValidForPrefix(keyPrefix); it.Seek(keyPrefix) {
			key := it.Item().KeyCopy(nil)
			if v, err := it.Item().ValueCopy(nil); err != nil {
				return newError(ErrCodeBadger, "failed to fetch value", err)
			} else {
				keys = append(keys, key)
				values = append(values, v)
			}
		}
		return nil
	})
	return
}

func (i *impl) DeleteScan(keyPrefix []byte) (keys [][]byte, err error) {
	err = i.db.Update(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.IteratorOptions{PrefetchValues: false})
		defer it.Close()
		if !it.Valid() {
			return newError(ErrCodeEmpty, "iterator finishes prematurely, empty database?", nil)
		}

		for it.Seek(keyPrefix); it.ValidForPrefix(keyPrefix); it.Seek(keyPrefix) {
			key := it.Item().KeyCopy(nil)
			if err := txn.Delete(key); err != nil {
				return newError(ErrCodeBadger, "failed to delete key", err)
			} else {
				keys = append(keys, key)
			}
		}
		return nil
	})
	return
}
