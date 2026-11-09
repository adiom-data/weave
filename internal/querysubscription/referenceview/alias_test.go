package referenceview

import (
	"context"
	"testing"

	"github.com/adiom-data/weave/internal/querysubscription/feedbuffer"
	"github.com/adiom-data/weave/internal/storage"
)

func TestLastAliasClosePrunesBucket(t *testing.T) {
	key := testKey("link")
	p := &parent{aliases: map[storage.EntityKey]map[string]*alias{}}
	f := &feed{parents: map[string]*parent{"parent": p}, aliases: map[string]*alias{}, events: feedbuffer.New(4)}
	for _, id := range []string{"one", "two"} {
		a := &alias{id: id, parent: "parent", key: key, feed: f}
		f.aliases[id] = a
		if p.aliases[key] == nil {
			p.aliases[key] = map[string]*alias{}
		}
		p.aliases[key][id] = a
	}
	one, two := f.aliases["one"], f.aliases["two"]
	if err := one.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.aliases[key]) != 1 || p.aliases[key]["two"] != two {
		t.Fatal("closing one alias removed its sibling")
	}
	if err := two.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.aliases) != 0 || len(f.aliases) != 0 {
		t.Fatal("last close retained alias bookkeeping")
	}
	if err := two.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
