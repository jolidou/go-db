package godb

import (
	"testing"
)

func TestInsertColumnPage(t *testing.T) {
	td, _, t1, t2, cf, _, _ := makeColumnTestVars()
	field1 := td.copy().Fields[0]
	pg := newColumnPage(&field1, 0, cf)
	var expectedSlots = (PageSize - 8) / (StringLength)
	if pg.getNumSlots() != expectedSlots {
		t.Fatalf("Incorrect number of slots, `expected %d, got %d", expectedSlots, pg.getNumSlots())
	}

	pg.insertTuple(&t1)
	pg.insertTuple(&t2)

	iter := pg.tupleIter()
	cnt := 0
	for {

		tup, _ := iter()
		if tup == nil {
			break
		}

		cnt += 1
	}
	if cnt != 2 {
		t.Errorf("Expected 2 tuples in interator, got %d", cnt)
	}
}

func TestDeleteColumnPage(t *testing.T) {
	td, _, t1, t2, cf, _, _ := makeColumnTestVars()
	field1 := td.copy().Fields[0]
	pg := newColumnPage(&field1, 0, cf)

	pg.insertTuple(&t1)
	slotNo, _ := pg.insertTuple(&t2)
	pg.deleteTuple(slotNo)

	iter := pg.tupleIter()
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	cnt := 0
	for {

		tup, _ := iter()
		if tup == nil {
			break
		}

		cnt += 1
	}
	if cnt != 1 {
		t.Errorf("Expected 1 tuple in interator, got %d", cnt)
	}
}

// Unit test for insertTuple
func TestColumnPageInsertTuple(t *testing.T) {
	td, field1, t1, _, cf, _, _ := makeColumnTestVars()
	page := newColumnPage(&field1, 0, cf)
	free := page.getNumSlots()

	for i := 0; i < free; i++ {
		var addition = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{"sam"},
			},
		}
		page.insertTuple(&addition)

		iter := page.tupleIter()
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		cnt, found := 0, false
		for {

			tup, _ := iter()
			found = found || addition.equals(tup)
			if tup == nil {
				break
			}

			cnt += 1
		}
		if cnt != i+1 {
			t.Errorf("Expected %d tuple in interator, got %d", i+1, cnt)
		}
		if !found {
			t.Errorf("Expected inserted tuple to be FOUND, got NOT FOUND")
		}
	}

	_, err := page.insertTuple(&t1)

	if err == nil {
		t.Errorf("Expected error due to full page")
	}
}

// Unit test for deleteTuple
func TestColumnPageDeleteTuple(t *testing.T) {
	_, field1, _, _, cf, _, _ := makeColumnTestVars()
	page := newColumnPage(&field1, 0, cf)
	free := page.getNumSlots()
	var tdFt []FieldType
	tdFt = append(tdFt, field1)

	list := make([]recordID, free)
	for i := 0; i < free; i++ {
		var addition = Tuple{
			Desc: TupleDesc{tdFt},
			Fields: []DBValue{
				StringField{"sam"},
			},
		}
		list[i], _ = page.insertTuple(&addition)
	}
	if len(list) == 0 {
		t.Fatalf("Rid list is empty.")
	}
	for i, rnd := free-1, 0xdefaced; i > 0; i, rnd = i-1, (rnd*0x7deface1+12354)%0x7deface9 {
		// Generate a random index j such that 0 <= j <= i.
		j := rnd % (i + 1)

		// Swap arr[i] and arr[j].
		list[i], list[j] = list[j], list[i]
	}

	for _, rid := range list {
		err := page.deleteTuple(rid)
		if err != nil {
			t.Errorf("Found error %s", err.Error())
		}
	}

	err := page.deleteTuple(list[0])
	if err == nil {
		t.Errorf("page should be empty; expected error")
	}
}

// Unit test for isDirty, setDirty
func TestColumnPageDirty(t *testing.T) {
	_,field1, _, _, cf, _, _ := makeColumnTestVars()
	page := newColumnPage(&field1, 0, cf)

	page.setDirty(true)
	if !page.isDirty() {
		t.Errorf("page should be dirty")
	}
	page.setDirty(true)
	if !page.isDirty() {
		t.Errorf("page should be dirty")
	}
	page.setDirty(false)
	if page.isDirty() {
		t.Errorf("page should be not dirty")
	}
}

// Unit test for toBuffer and initFromBuffer
func TestColumnPageSerialization(t *testing.T) {

	_, field1, _, _, cf, _, _ := makeColumnTestVars()
	page := newColumnPage(&field1, 0, cf)
	free := page.getNumSlots()
	var tdFt []FieldType
	tdFt = append(tdFt, field1)

	for i := 0; i < free-1; i++ {
		var addition = Tuple{
			Desc: TupleDesc{tdFt},
			Fields: []DBValue{
				StringField{"sam"},
			},
		}
		page.insertTuple(&addition)
	}

	buf, _ := page.toBuffer()
	page2 := newColumnPage(&field1, 0, cf)
	err := page2.initFromBuffer(buf)
	if err != nil {
		t.Fatalf("Error loading column page from buffer.")
	}

	iter, iter2 := page.tupleIter(), page2.tupleIter()
	if iter == nil {
		t.Fatalf("iter was nil.")
	}
	if iter2 == nil {
		t.Fatalf("iter2 was nil.")
	}

	findEqCount := func(t0 *Tuple, iter3 func() (*Tuple, error)) int {
		cnt := 0
		for tup, _ := iter3(); tup != nil; tup, _ = iter3() {
			if t0.equals(tup) {
				cnt += 1
			}
		}
		return cnt
	}

	for {
		tup, _ := iter()
		if tup == nil {
			break
		}
		ct1 := findEqCount(tup, page.tupleIter())
		ct2 := findEqCount(tup, page2.tupleIter())
		if ct1 != ct2 {
			t.Errorf("Serialization / deserialization doesn't result in identical column page.")
		}
	}
}
