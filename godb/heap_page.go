package godb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
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
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	HeapFile HeapFile
	PageNo   int
	Desc     TupleDesc
	Tuples   []*Tuple
	Dirty    bool
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	bytesPerTuple := 0
	for i := 0; i < len(desc.Fields); i++ {
		if desc.Fields[i].Ftype == 0 {
			bytesPerTuple += int(unsafe.Sizeof(int64(0)))
		}
		if desc.Fields[i].Ftype == 1 {
			bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
		}
	}

	remPageSize := PageSize - 8
	numSlots := remPageSize / bytesPerTuple
	tuples := make([]*Tuple, numSlots)
	return &heapPage{HeapFile: *f, PageNo: pageNo, Desc: *desc, Tuples: tuples, Dirty: false}
}

func (h *heapPage) getNumSlots() int {
	bytesPerTuple := 0
	for i := 0; i < len(h.Desc.Fields); i++ {
		if h.Desc.Fields[i].Ftype == 0 {
			bytesPerTuple += int(unsafe.Sizeof(int64(0)))
		}
		if h.Desc.Fields[i].Ftype == 1 {
			bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
		}
	}

	remPageSize := PageSize - 8
	numSlots := remPageSize / bytesPerTuple
	return numSlots //replace me
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	rid := []int{}
	free := false
	used := 0
	for i, tup := range h.Tuples {
		if tup != nil {
			used += 1
		}
		if tup == nil {
			rid = append(rid, h.PageNo, i)
			t.Rid = rid
			h.Tuples[i] = t
			free = true
			return rid, nil
		}
		if tup.Rid == nil {
			rid = append(rid, h.PageNo, i)
			tup.Rid = rid
		}
	}
	// at slot capacity
	if !free && (used == h.getNumSlots()) {
		return nil, errors.New("no free slots")
	}

	if used != h.getNumSlots() {
		rid = append(rid, h.PageNo)
		rid = append(rid, used)
		t.Rid = rid
		h.Tuples[used] = t
		free = true
	}

	if !free {
		return nil, errors.New("no free slots")
	}
	return rid, nil //replace me
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	if ridSlice, ok := rid.([]int); ok {
		if ridSlice[1] < 0 || ridSlice[1] >= len(h.Tuples) {
			return errors.New("slot invalid")
		}

		if ridSlice[0] != h.PageNo {
			return errors.New("wrong page")
		}

		count := 0
		for i, tup := range h.Tuples {
			// Count tup if it is non-nil
			if tup != nil {
				// Otherwise, count non-nil value
				count++
				if tup.Rid != nil {
					tupRid, _ := tup.Rid.([]int)
					// Delete tuple if its RID matches rid and return nil
					if tupRid[1] == ridSlice[1] {
						h.Tuples[i] = nil
						return nil
					}
				}
			}
		}
		return errors.New("did not find tuple to delete")
	}

	return nil //replace me
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	return h.Dirty
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	h.Dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	var f DBFile = &p.HeapFile
	return &f
}

// }

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	buffer := new(bytes.Buffer)
	buffer.Grow(4096)
	numSlots := len(h.Tuples)
	count := 0
	for _, item := range h.Tuples {
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

	t := 0
	// Iterate through the tuples of the page and write them to the buffer
	for _, tuple := range h.Tuples {
		if tuple != nil {
			if err := tuple.writeTo(buffer); err != nil {
				t += 1
				return nil, err
			}
		}
	}

	// Calculate the number of remaining slots to fill with zeros
	remainingSlots := 4096 - buffer.Len()

	// Use bytes.Repeat to fill the remaining slots with zeros
	zeroBytes := bytes.Repeat([]byte{0}, remainingSlots)
	buffer.Write(zeroBytes)

	//todo: pad the rest of the buffer?
	return buffer, nil //replace me

}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	var totalNumSlots int32
	if err := binary.Read(buf, binary.LittleEndian, &totalNumSlots); err != nil {
		return err
	}
	var totalUsedSlots int32
	if err := binary.Read(buf, binary.LittleEndian, &totalUsedSlots); err != nil {
		return err
	}
	bytesPerTuple := 0
	for i := 0; i < len(h.Desc.Fields); i++ {
		if h.Desc.Fields[i].Ftype == 0 {
			bytesPerTuple += int(unsafe.Sizeof(int64(0)))
		}
		if h.Desc.Fields[i].Ftype == 1 {
			bytesPerTuple += ((int)(unsafe.Sizeof(byte('a')))) * StringLength
		}
	}

	//toRead := int(totalUsedSlots) * bytesPerTuple
	destBuffer := &bytes.Buffer{}

	// Read bytesPerTuple bytes from the source buffer into the destination buffer
	for i := 0; i < int(totalUsedSlots); i++ {
		if i > h.getNumSlots() {
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
		tuple, err := readTupleFrom(destBuffer, &h.Desc)
		if err != nil {
			return err
		}
		if tuple != nil {
			h.Tuples[i] = tuple
		}
	}

	return nil //replace me
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
// func (p *heapPage) tupleIter() func() (*Tuple, error) {
// 	// TODO: some code goes here
// 	index := 0
// 	iterator := func() (*Tuple, error) {
// 		if index < len(p.Tuples) {
// 			currTuple := p.Tuples[index]
// 			rid := []int{}
// 			rid = append(rid, p.PageNo)
// 			rid = append(rid, index)
// 			ret := &Tuple{}
// 			if currTuple != nil {
// 				ret = &Tuple{Desc: currTuple.Desc, Fields: currTuple.Fields, Rid: rid}
// 			} else {
// 				ret = nil
// 			}
// 			index++
// 			return ret, nil
// 		}

// 		return nil, nil
// 	}

// 	return iterator //replace me
// }

func (p *heapPage) tupleIter() func() (*Tuple, error) {
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

		return nil, errors.New("end of iteration")

	}
}
