package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"unsafe"
)

/* ColumnPage implements the Page interface for pages of ColumnFiles. We have
provided our interface to ColumnPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [ColumnFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, whicc means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin witc a header witc a 32
bit integer witc the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Eacc tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLengtc bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the column page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type columnPage struct {
	ColumnFile ColumnFile
	Field      *FieldType
	Tuples     []*Tuple
	PageNo     int
	Dirty      bool
}

// Construct a new column page
func newColumnPage(field *FieldType, pageNo int, f *ColumnFile) *columnPage {
	bytesPerTuple := 0
	if field.Ftype == 0 {
		bytesPerTuple += int(unsafe.Sizeof(int64(0)))
	}
	if field.Ftype == 1 {
		bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
	}

	remPageSize := PageSize - 8
	numSlots := remPageSize / bytesPerTuple
	tuples := make([]*Tuple, numSlots)
	return &columnPage{ColumnFile: *f, PageNo: pageNo, Field: field, Tuples: tuples, Dirty: false}
}

func (p *columnPage) getNumSlots() int {
	bytesPerTuple := 0
	if p.Field.Ftype == 0 {
		bytesPerTuple += int(unsafe.Sizeof(int64(0)))
	}
	if p.Field.Ftype == 1 {
		bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
	}

	remPageSize := PageSize - 8
	numSlots := remPageSize / bytesPerTuple
	return numSlots
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (p *columnPage) insertTuple(t *Tuple) (recordID, error) {
	//fmt.Println("HEAP PAGE insert tuple")
	rid := rID{}
	free := false
	used := 0
	for i, tup := range p.Tuples {
		if tup != nil {
			used += 1
		}
		if tup == nil {
			rid = rID{Page: p.PageNo, Slot: i}
			t.Rid = rid
			p.Tuples[i] = t
			free = true
			return rid, nil
		}
		if tup.Rid == nil {
			rid = rID{Page: p.PageNo, Slot: i}
			tup.Rid = rid
			//fmt.Println("INSERT TUP 2:", tup.Fields[0], len(tup.Fields), tup.Rid)
		}
	}
	// at slot capacity
	if !free {
		return nil, errors.New("no free slots")
	}
	return nil, nil
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (p *columnPage) deleteTuple(rid recordID) error {
	if rid, ok := rid.(rID); ok {
		if rid.Slot < 0 || rid.Slot >= len(p.Tuples) {
			return errors.New("slot invalid")
		}

		if rid.Page != p.PageNo {
			return errors.New("wrong page")
		}

		count := 0
		for i, tup := range p.Tuples {
			// Count tup if it is non-nil
			if tup != nil {
				// Otherwise, count non-nil value
				count++
				if tup.Rid != nil {
					tupRid, _ := tup.Rid.(rID)
					// Delete tuple if its RID matches rid and return nil
					if tupRid.Slot == rid.Slot {
						p.Tuples[i] = nil
						return nil
					}
				}
			}
		}
		return errors.New("did not find tuple to delete")
	}

	return errors.New("rid did not have correct format")
}

// Page method - return whether or not the page is dirty
func (p *columnPage) isDirty() bool {
	return p.Dirty
}

// Page method - mark the page as dirty
func (p *columnPage) setDirty(dirty bool) {
	p.Dirty = dirty
}

// Page method - return the corresponding ColumnFile
// for this page.
func (p *columnPage) getFile() *DBFile {
	var f DBFile = &p.ColumnFile
	return &f
}

// }

// Allocate a new bytes.Buffer and write the column page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [ColumnFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (p *columnPage) toBuffer() (*bytes.Buffer, error) {
	buffer := new(bytes.Buffer)
	buffer.Grow(PageSize)
	numSlots := len(p.Tuples)
	count := 0
	for _, item := range p.Tuples {
		if item != nil {
			count++
		}
	}

	// page header
	if err := binary.Write(buffer, binary.LittleEndian, int32(numSlots)); err != nil {
		return nil, err
	}

	if err := binary.Write(buffer, binary.LittleEndian, int32(count)); err != nil {
		return nil, err
	}

	// Iterate througc the tuples of the page and write them to the buffer
	for _, tuple := range p.Tuples {
		if tuple != nil {
			if err := tuple.writeTo(buffer); err != nil {
				fmt.Println("ERROR:", err)
				return nil, err
			}
		}
	}

	// Calculate the number of remaining slots to fill witc zeros
	remainingSlots := 4096 - buffer.Len()
	fmt.Println("remaining", remainingSlots)
	fmt.Println(buffer)
	// Use bytes.Repeat to fill the remaining slots witc zeros
	zeroBytes := bytes.Repeat([]byte{0}, remainingSlots)
	buffer.Write(zeroBytes)

	return buffer, nil

}

// Read the contents of the ColumnPage from the supplied buffer.
func (p *columnPage) initFromBuffer(buf *bytes.Buffer) error {
	var totalNumSlots int32
	if err := binary.Read(buf, binary.LittleEndian, &totalNumSlots); err != nil {
		return err
	}
	var totalUsedSlots int32
	if err := binary.Read(buf, binary.LittleEndian, &totalUsedSlots); err != nil {
		return err
	}
	bytesPerTuple := 0
	if p.Field.Ftype == 0 {
		bytesPerTuple += int(unsafe.Sizeof(int64(0)))
	}
	if p.Field.Ftype == 1 {
		bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
	}

	destBuffer := &bytes.Buffer{}
	var tupFields []FieldType
	tupFields = append(tupFields, *p.Field)
	tupleDesc := TupleDesc{tupFields}
	//j := 0
	// Read bytesPerTuple bytes from the source buffer into the destination buffer
	for i := 0; i < int(totalUsedSlots); i++ {
		if i > p.getNumSlots() {
			break
		}
		// Read bytesPerTuple bytes from the source buffer
		chunk := make([]byte, bytesPerTuple)
		_, err := buf.Read(chunk)
		if err != nil {
			break // Exit the loop when there's an error (e.g., end of data)
		}

		// Write the read chunk to the destination buffer
		destBuffer.Write(chunk[:])
		tuple, err := readTupleFrom(destBuffer, &tupleDesc)

		if err != nil {
			return err
		}
		if tuple != nil {
			rid := rID{Page: p.PageNo, Slot: i}
			tuple.Rid = rid
			p.Tuples[i] = tuple
		}
	}

	return nil
}

func (p *columnPage) tupleIter() func() (*Tuple, error) {
	i := 0
	return func() (*Tuple, error) {
		for i < len(p.Tuples) {
			s := p.Tuples[i]
			i++ // Increment the iterator regardless of the element

			if s == nil {
				continue // Skip nil elements
			}

			return s, nil
		}
		// Reset the iterator to the beginning when reaching the end
		i = 0

		return nil, nil

	}
}
