package godb

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

func makeLargeColumnTestVars() (TupleDesc, []FieldType, Tuple, Tuple, *ColumnFile, *ColumnBufferPool, TransactionID, int) {
	var largeTd = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "surname", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
		{Fname: "weight", Ftype: IntType},
		{Fname: "height", Ftype: IntType},
		{Fname: "fav_fruit", Ftype: StringType},
		{Fname: "fav_color", Ftype: StringType},
		{Fname: "pet", Ftype: StringType},
		{Fname: "state", Ftype: StringType},
		{Fname: "fav_book", Ftype: StringType},
		{Fname: "family_size", Ftype: IntType},
		{Fname: "shoe_size", Ftype: IntType},
	}}

	// goal
	var want = []FieldType{
		FieldType{Fname: "name", Ftype: StringType},
		FieldType{Fname: "age", Ftype: IntType},
	}

	var largeT1 = Tuple{
		Desc: largeTd,
		Fields: []DBValue{
			StringField{"sam"},
			StringField{"smith"},
			IntField{10},
			IntField{180},
			IntField{65},
			StringField{"apple"},
			StringField{"red"},
			StringField{"horse"},
			StringField{"california"},
			StringField{"odyssey"},
			IntField{5},
			IntField{9},
		}}

	var largeT2 = Tuple{
		Desc: largeTd,
		Fields: []DBValue{
			StringField{"mark"},
			StringField{"zuckerberg"},
			IntField{50},
			IntField{170},
			IntField{67},
			StringField{"plum"},
			StringField{"blue"},
			StringField{"wolf"},
			StringField{"california"},
			StringField{"meditations"},
			IntField{4},
			IntField{10},
		}}
	bp := NewColumnBufferPool(20)
	os.Remove(TestingFile)
	largeCf, err := NewColumnFile(TestingFile, &largeTd, bp)
	n := 5
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	tid := NewTID()
	bp.BeginTransaction(tid)

	return largeTd, want, largeT1, largeT2, largeCf, bp, tid, n
}

func makeColumnTestVars() (TupleDesc, FieldType, Tuple, Tuple, *ColumnFile, *ColumnBufferPool, TransactionID) {
	var td = TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}

	var field1 = FieldType{Fname: "name", Ftype: StringType}

	var t1 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"sam"},
			IntField{25},
		}}

	var t2 = Tuple{
		Desc: td,
		Fields: []DBValue{
			StringField{"george jones"},
			IntField{999},
		}}

	bp := NewColumnBufferPool(3)
	os.Remove(TestingFile)
	cf, err := NewColumnFile(TestingFile, &td, bp)
	if err != nil {
		print("ERROR MAKING TEST VARS, BLARGH")
		panic(err)
	}

	tid := NewTID()
	bp.BeginTransaction(tid)

	return td, field1, t1, t2, cf, bp, tid

}

func TestCreateAndInsertColumnFile(t *testing.T) {
	_, _, t1, t2, cf, _, tid := makeColumnTestVars()
	cf.insertTuple(&t1, tid)
	cf.insertTuple(&t2, tid)
	iter, _ := cf.Iterator(tid)
	i := 0
	for {
		t, err := iter()
		fmt.Println("TUPLE", t)
		if err != nil {
			fmt.Println(err)
		}
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 4 {
		t.Errorf("ColumnFile iterator expected 2 tuples, got %d", i)
	}
}

func TestDeleteColumnFile(t *testing.T) {
	_, _, t1, t2, cf, _, tid := makeColumnTestVars()
	cf.insertTuple(&t1, tid)
	cf.insertTuple(&t2, tid)
	fmt.Println("rid", t1.Rid)

	cf.deleteTuple(&t1, tid)
	iter, _ := cf.Iterator(tid)
	t3, _ := iter()
	if t3 == nil {
		t.Errorf("HeapFile iterator expected 1 tuple")
	}
	cf.deleteTuple(&t2, tid)
	iter, _ = cf.Iterator(tid)
	t3, _ = iter()
	if t3 != nil {
		fmt.Println("t3", t3)
		t.Errorf("HeapFile iterator expected 0 tuple")
	}
}

func TestColumnLoadWideCSV(t *testing.T) {
	startTime := time.Now()
	_, want, _, _, largeCf, _, tid, _ := makeLargeColumnTestVars()

	f, err := os.Open("test_column_file.csv")
	if err != nil {
		t.Errorf("Couldn't open test_column_file.csv")
		return
	}
	err = largeCf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		t.Fatalf("Load failed, %s", err)
	}
	fmt.Println("LOADED LARGE CSV")
	//should have 50 tuples, only with name and age
	iter, _ := largeCf.ColumnIterator(want, tid)
	i := 0
	for {
		t, _ := iter()
		// fmt.Println(i, t)
		if t == nil {
			break
		}
		// Expect 2 columns: name and age ONLY
		if len(t.Desc.Fields) != 2 {
			errors.New("Expected output tuples to have 2 columns")
		}
		i = i + 1
	}
	// Expect 50 tuples (# of rows in file)
	if i != 50 {
		t.Errorf("ColumnFile iterator expected 50 tuples, got %d", i)
	}
	// Record the end time
	endTime := time.Now()

	// Calculate the elapsed time
	elapsedTime := endTime.Sub(startTime)

	// Print the elapsed time
	fmt.Printf("Elapsed time col: %s\n", elapsedTime)
	// t.Errorf("uncomment this to see runtime")
}

func TestHeapLoadWideCSV(t *testing.T) {
	startTime := time.Now()
	largeTd, _, _, _, _, _, tid, _ := makeLargeColumnTestVars()

	bp := NewBufferPool(20)
	os.Remove(TestingFile)
	hf, err := NewHeapFile(TestingFile, &largeTd, bp)

	f, err := os.Open("test_column_file.csv")
	if err != nil {
		t.Errorf("Couldn't open test_column_file.csv")
		return
	}
	err = hf.LoadFromCSV(f, true, ",", false)
	if err != nil {
		t.Fatalf("Load failed, %s", err)
	}
	fmt.Println("LOADED LARGE CSV")
	//should have 50 tuples
	iter, _ := hf.Iterator(tid)
	i := 0
	for {
		t, _ := iter()
		fmt.Println("t", t)
		// fmt.Println(i, t)
		if t == nil {
			break
		}
		i = i + 1
	}
	if i != 50 {
		t.Errorf("ColumnFile iterator expected 50 tuples, got %d", i)
	}
	// Record the end time
	endTime := time.Now()

	// Calculate the elapsed time
	elapsedTime := endTime.Sub(startTime)

	// Print the elapsed time
	fmt.Printf("Elapsed time heap: %s\n", elapsedTime)
	// t.Errorf("uncomment this to see runtime")
}
