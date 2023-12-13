package godb

type InsertOp struct {
	file  DBFile
	child Operator
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	return &InsertOp{insertFile, child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	var fields []FieldType
	desc := *i.child.Descriptor()
	f := FieldType{Fname: "count", TableQualifier: desc.Fields[0].TableQualifier, Ftype: IntType}
	fields = append(fields, f)
	return &TupleDesc{fields}
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	ct := 0
	iter, err := iop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {
		for {
			t, err := iter()
			if t == nil {
				var fields []DBValue
				ctField := IntField{int64(ct)}
				fields = append(fields, ctField)
				out := &Tuple{*iop.Descriptor().copy(), fields, 0}
				return out, nil
			}
			if err != nil {
				return nil, err
			}
			err = iop.file.insertTuple(t, tid)
			if err != nil {
				return nil, err
			}
			ct++
		}
	}, nil

}
