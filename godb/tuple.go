package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

var typeNames map[DBType]string = map[DBType]string{IntType: "int", StringType: "string"}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	if len(d1.Fields) != len(d2.Fields) {
		return false
	}
	for i := range d1.Fields {
		if d1.Fields[i].Fname != d2.Fields[i].Fname {
			return false
		}
		if d1.Fields[i].TableQualifier != d2.Fields[i].TableQualifier {
			return false
		}
		if d1.Fields[i].Ftype != d2.Fields[i].Ftype {
			return false
		}
	}
	return true

}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}

}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	tupleDescCopy := make([]FieldType, len(td.Fields))
	copy(tupleDescCopy, td.Fields)
	return &TupleDesc{Fields: tupleDescCopy}
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	merged := append(desc.Fields, desc2.Fields...)
	return &TupleDesc{Fields: merged}
}

// ================== Tuple Methods ======================

// Interface used for tuple field values
// Since it implements no methods, any object can be used
// but having an interface for this improves code readability
// where tuple values are used
type DBValue interface {
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
}

type rID struct {
	Page int
	Slot int
}

type rrID struct {
	Page []int
	Slot []int
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	for _, field := range t.Fields {
		if strVal, ok := field.(StringField); ok {
			strBytes := []byte(strVal.Value)
			padded := make([]byte, StringLength)
			copy(padded, strBytes)
			err := binary.Write(b, binary.LittleEndian, padded)
			if err != nil {
				return err
			}
		} else if intVal, ok := field.(IntField); ok {
			val := int64(intVal.Value)
			err := binary.Write(b, binary.LittleEndian, val)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	tuple := &Tuple{Desc: *desc}

	for _, field := range desc.Fields {
		if field.Ftype == 1 {
			f := make([]byte, StringLength)
			err := binary.Read(b, binary.LittleEndian, f)
			if err != nil {
				return nil, err
			}
			trimmed := strings.TrimRight(string(f), "\x00")
			tuple.Fields = append(tuple.Fields, StringField{trimmed})
		} else {
			var f int64
			err := binary.Read(b, binary.LittleEndian, &f)
			if err != nil {
				return nil, err
			}
			tuple.Fields = append(tuple.Fields, IntField{f})
		}
	}
	return tuple, nil

}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	if t1 == nil && t2 == nil {
		return true
	}
	if t1 == nil || t2 == nil {
		return false
	}

	if !t1.Desc.equals(&t2.Desc) {
		return false
	}

	if len(t1.Fields) != len(t2.Fields) {
		return false
	}

	for i := range t1.Fields {
		if t1.Fields[i] != t2.Fields[i] {
			return false
		}
	}

	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2 appended to t1.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	if t1 == nil {
		return t2
	}
	if t2 == nil {
		return t1
	}
	mergedDesc := TupleDesc{
		Fields: append(t1.Desc.Fields, t2.Desc.Fields...),
	}

	mergedFields := append(t1.Fields, t2.Fields...)

	return &Tuple{
		Desc:   mergedDesc,
		Fields: mergedFields,
	}
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	val1, err := field.EvalExpr(t)
	if err != nil {
		return OrderedEqual, err
	}
	val2, err := field.EvalExpr(t2)
	if err != nil {
		return OrderedEqual, err
	}

	if v1, ok := val1.(IntField); ok {
		if v2, ok := val2.(IntField); ok {
			if v1.Value == v2.Value {
				return OrderedEqual, nil
			} else if v1.Value > v2.Value {
				return OrderedGreaterThan, nil
			} else {
				return OrderedLessThan, nil
			}
		}
	}

	if v1, ok := val1.(StringField); ok {
		if v2, ok := val2.(StringField); ok {
			if v1.Value < v2.Value {
				return OrderedLessThan, nil
			} else if v1.Value > v2.Value {
				return OrderedGreaterThan, nil
			} else {
				return OrderedEqual, nil
			}
		}
	}
	return OrderedEqual, err
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	projected := &Tuple{}

	add := []int{}
	for _, field := range fields {
		for i, t := range t.Desc.Fields {
			if field.Fname == t.Fname && field.TableQualifier == t.TableQualifier {
				add = append(add, i)
			} else if field.Fname == t.Fname {
				add = append(add, i)
			}
		}
	}

	for _, i := range add {
		projected.Fields = append(projected.Fields, t.Fields[i])
	}

	return projected, nil
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {

	//todo efficiency here is poor - hashstructure is probably slow
	hash, _ := hashstructure.Hash(t, hashstructure.FormatV2, nil)

	return hash
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = fmt.Sprintf("%d", f.Value)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr

}

func (f1 FieldType) fieldEquals(f2 FieldType) bool {
	return (f1.Fname == f2.Fname && f1.Ftype == f2.Ftype && f1.TableQualifier == f2.TableQualifier)
}
