package mdatabase

import (
	"testing"

	"github.com/uptrace/bun"
)

func TestRelationStoresAndAppliesCallback(t *testing.T) {
	query := Query[struct{}](nil)
	applied := false
	callback := func(selectQuery *bun.SelectQuery) *bun.SelectQuery {
		applied = true
		return selectQuery
	}

	query.Relation("Images", callback)

	if len(query.relations) != 1 {
		t.Fatalf("relation count = %d, want 1", len(query.relations))
	}
	if query.relations[0].name != "Images" {
		t.Fatalf("relation name = %q, want %q", query.relations[0].name, "Images")
	}
	query.relations[0].apply(nil)
	if !applied {
		t.Fatal("relation callback was not retained")
	}
}

func TestRelationWithoutCallbackRemainsSupported(t *testing.T) {
	query := Query[struct{}](nil).Relation("Images")

	if len(query.relations) != 1 {
		t.Fatalf("relation count = %d, want 1", len(query.relations))
	}
	if query.relations[0].apply != nil {
		t.Fatal("relation without callback should not have an apply function")
	}
}
