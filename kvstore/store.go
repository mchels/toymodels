package kvstore

type Store struct {
	store map[string]string
}

func NewStore() *Store {
	return &Store{store: make(map[string]string)}
}

func (store *Store) Get(key string) (string, bool) {
	val, ok := store.store[key]
	return val, ok
}

func (store *Store) Put(key, value string) {
	store.store[key] = value
}

func (store *Store) Delete(key string) {
	delete(store.store, key)
}
