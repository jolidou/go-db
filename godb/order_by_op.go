package godb

import (
	"sort"
)

// TODO: some code goes here
type OrderBy struct {
	orderBy   []Expr // OrderBy should include these two fields (used by parser)
	child     Operator
	ascending []bool
	tups      []*Tuple
	//add additional fields here
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
	var tups []*Tuple
	return &OrderBy{orderByFields, child, ascending, tups}, nil

}

// Sort sorts the argument slice according to the less functions passed to OrderedBy.
func (ob *OrderBy) Sort(tups []*Tuple) {
	ob.tups = tups
	sort.Sort(ob)
}

// Len is part of sort.Interface.
func (ob *OrderBy) Len() int {
	return len(ob.tups)
}

// Swap is part of sort.Interface.
func (ob *OrderBy) Swap(i, j int) {
	ob.tups[i], ob.tups[j] = ob.tups[j], ob.tups[i]
}

// Less is part of sort.Interface. It is implemented by looping along the
// less functions until it finds a comparison that discriminates between
// the two items (one is less than the other). Note that it can call the
// less functions twice per call.
func (ob *OrderBy) Less(i, j int) bool {
	// Try all but the last comparison.
	t1, t2 := ob.tups[i], ob.tups[j]
	var k int
	for k = 0; k < len(ob.orderBy)-1; k++ {
		asc := ob.ascending[k]
		expr := ob.orderBy[k]
		obState, _ := t1.compareField(t2, expr)
		switch obState {
		case OrderedLessThan:
			// p < q, so we have a decision.
			if !asc {
				return false
			}
			return true
		case OrderedGreaterThan:
			// p > q, so we have a decision.
			if !asc {
				return true
			}
			return false
		}
	}
	asc := ob.ascending[k]
	expr := ob.orderBy[k]
	obState, _ := t1.compareField(t2, expr)
	if obState == OrderedLessThan {
		return asc
	}
	if obState == OrderedGreaterThan {
		return !asc
	}
	return true
}

func (ob *OrderBy) Descriptor() *TupleDesc {
	return ob.child.Descriptor().copy()
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (ob *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// Get all tuples
	iter, err := ob.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	for {
		tup, err := iter()
		if tup == nil {
			break
		}
		if err != nil {
			return nil, err
		}
		ob.tups = append(ob.tups, tup)
	}
	ob.Sort(ob.tups)

	// return iterator
	i := 0
	return func() (*Tuple, error) {
		for {
			if i == len(ob.tups) {
				return nil, nil
			}
			c := i
			i += 1
			return ob.tups[c], nil
		}
	}, nil
}
