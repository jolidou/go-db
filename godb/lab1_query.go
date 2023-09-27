package godb

import (
	"errors"
	"os"
)

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	// os remove
	os.Remove("new_heap_file.dat")
	bp := NewBufferPool(3)

	hf, err := NewHeapFile("new_heap_file.dat", &td, bp)
	if err != nil {
		return 0, err
	}

	var file *os.File
	file, err = os.Open(fileName)
	if err != nil {
		return 0, err
	}
	err = hf.LoadFromCSV(file, true, ",", false)
	if err != nil {
		return 0, err
	}

	field, err := findFieldInTd(FieldType{Fname: sumField}, &td)
	if err != nil {
		return 0, err
	}
	sum := 0
	tid := NewTID()
	iter, _ := hf.Iterator(tid)
	i := 0
	for {
		t, _ := iter()
		if t != nil {
			intVal, isInt := t.Fields[field].(IntField)
			if !isInt {
				return 0, errors.New("expected integer value")
			}
			sum += int(intVal.Value)
		}

		if t == nil {
			break
		}
		i = i + 1
	}
	return sum, nil

}
