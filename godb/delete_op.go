package godb

type DeleteOp struct {
	file DBFile
	child Operator
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	return &DeleteOp{deleteFile, child}
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	var fields []FieldType
	desc := *i.child.Descriptor()
	f := FieldType{Fname: "count", TableQualifier: desc.Fields[0].TableQualifier, Ftype: IntType}
	fields = append(fields, f)
	return &TupleDesc{fields}
}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	ct := 0
	iter, err := dop.child.Iterator(tid)
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
				out := &Tuple{*dop.Descriptor().copy(), fields, 0}
				return out, nil
			}
			if err != nil {
				return nil, err
			}
			err = dop.file.deleteTuple(t, tid)
			if err != nil {
				return nil, err
			}
			ct++
		}
	}, nil
}
