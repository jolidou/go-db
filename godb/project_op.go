package godb

import (
	"errors"
)

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	distinct     bool
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	if len(selectFields) != len(outputNames) {
		return nil, errors.New("mismatched length")
	}
	return &Project{selectFields, outputNames, child, distinct}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	var fields []FieldType
	for i, expr := range p.selectFields {
		fieldType := expr.GetExprType()
		fieldType.Fname = p.outputNames[i]
		fields = append(fields, fieldType)
	}
	return &TupleDesc{Fields: fields}
}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	iter, err := p.child.Iterator(tid)
	//i := 0
	if err != nil {
		return nil, err
	}
	var seen []*Tuple

	return func() (*Tuple, error) {
		for {
			t, err := iter()
			if t == nil {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}

			projected := make([]DBValue, len(p.selectFields))
			for i, expr := range p.selectFields {
				field, err := expr.EvalExpr(t)
				if err != nil {
					return nil, err
				}
				projected[i] = field
			}

			tt := &Tuple{Desc: *p.Descriptor(), Fields: projected}
			if p.distinct {
				sawT := false
				for _, seenT := range seen {
					if tt.equals(seenT) {
						sawT = true
						break
					}
				}
				if sawT {
					continue
				}
				seen = append(seen, tt)
			}
			return tt, nil
		}
	}, nil
}
