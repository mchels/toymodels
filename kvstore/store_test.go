package kvstore

import "testing"

func TestPutAndGet(t *testing.T) {
	store := NewStore()

	store.Put("foo", "bar")

	val, ok := store.Get("foo")
	if !ok {
		t.Fatal("expected key 'foo' to exist")
	}
	if val != "bar" {
		t.Errorf("expected 'bar', got '%s'", val)
	}
}

func TestGetMissing(t *testing.T) {
	store := NewStore()

	_, ok := store.Get("nonexistent")
	if ok {
		t.Fatal("expected key 'nonexistent' to not exist")
	}
}

func TestDelete(t *testing.T) {
	store := NewStore()

	store.Put("foo", "bar")
	store.Delete("foo")

	_, ok := store.Get("foo")
	if ok {
		t.Fatal("expected key 'foo' to be deleted")
	}
}
