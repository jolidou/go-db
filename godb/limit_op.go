package godb

import "errors"

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	limit int
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	// limit is a constant, so with any tuple, it'll evaluate to the same number - find that limit and store it
	empty := make([]DBValue, 0)
	emptyDesc := TupleDesc{}
	dummy := Tuple{emptyDesc, empty, 0}
	limit, _:= lim.EvalExpr(&dummy)
	limInt, _ := limit.(IntField)
	return &LimitOp{child, lim, int(limInt.Value)}
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	desc := *l.child.Descriptor()
	if len(desc.Fields) < l.limit {
		// m (# fields in child) < limit n, so resulting tuple desc has at most m fields
		return desc.copy()
	}
	// otherwise, get first n fields of desc
	return &TupleDesc{desc.Fields[:l.limit+1]}
}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	ct := 0
	iter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}

	return func() (*Tuple, error) {
		for {
			t, err := iter()
			if t == nil {
				return nil, nil
			}
			if err != nil {
				return nil, err
			}
			lim, err := l.limitTups.EvalExpr(t)
			if err != nil {
				return nil, err
			}
			limInt, ok := lim.(IntField)
			if !ok {
				return nil, errors.New("expected integer limit")
			}
			if int(limInt.Value) == ct {
				return nil, err
			} else {
				ct++
				return t, nil
			}
		}
	}, nil
}
