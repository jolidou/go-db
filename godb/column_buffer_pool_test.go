package godb

import (
	"testing"
)

func TestColumnGetPage(t *testing.T) {
	_, _, t1, t2, cf, bp, _, n := makeLargeColumnTestVars()
	tid := NewTID()
	for i := 0; i < n; i++ {
		err := cf.insertTuple(&t1, tid)
		if err != nil {
			t.Fatalf("%v", err)
		}
		err = cf.insertTuple(&t2, tid)
		if err != nil {
			t.Fatalf("%v", err)
		}
		// hack to force dirty pages to disk
		// because CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2
		for i := 0; i < cf.NumPages(); i++ {
			pg, err := bp.GetPage(cf, i, tid, ReadPerm)
			if pg == nil || err != nil {
				t.Fatal("page nil or error", err)
			}
			if (*pg).isDirty() {
				(*(*pg).getFile()).flushPage(pg)
				(*pg).setDirty(false)
			}
		}
	}

	pg, err := bp.GetPage(cf, 0, tid, ReadPerm)
	if pg == nil || err != nil {
		t.Fatalf("failed to get page %d (err = %v)", 0, err)
	}

	_, err1 := bp.GetPage(cf, 1, tid, ReadPerm)
	if err1 == nil {
		t.Fatalf("No error when getting page 7 from a file with 6 pages.")
	}
}
