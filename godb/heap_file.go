package godb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// HeapFile is an unordered collection of tuples Internally, it is arranged as a
// set of heapPage objects
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	Filename  string
	Desc      TupleDesc
	bufPool   *BufferPool
	currPages int
	sync.Mutex
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	var file *os.File
	file, err := os.Open(fromFile)
	if err != nil {
		file, err = os.Create(fromFile)
		if err != nil {
			return nil, err
		}
	}
	fileInfo, _ := file.Stat()

	// Get the file size in bytes
	fileSize := fileInfo.Size()

	numPages := int(fileSize) / PageSize
	return &HeapFile{Filename: fromFile, Desc: *td, bufPool: bp, currPages: numPages}, nil
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// Stat the file
	file, _ := os.Open(f.Filename)

	fileInfo, _ := file.Stat()

	// Get the file size in bytes
	fileSize := fileInfo.Size()

	numPages := int(fileSize) / PageSize

	return int(numPages)
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] is implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		f.bufPool.BeginTransaction(tid)
		f.insertTuple(&newT, tid)
		//commit frequently, to avoid all pages in BP being full
		//todo fix
		f.bufPool.CommitTransaction(tid)
	}
	return nil
}

// Read the specified page number from the HeapFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [heapPage] object, using
// the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (*Page, error) {
	// Create a buffer to store the page data
	data := make([]byte, PageSize)

	// if Page (zero-indexed) is out of bounds of current file, return error
	if pageNo >= f.NumPages() {
		return nil, errors.New("can't read page - out of file bounds")
	}

	// calculate correct offset
	offset := pageNo * PageSize

	file, err := os.Open(f.Filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	off, err := file.Seek(int64(offset), 0)
	if err != nil {
		return nil, err
	}

	// read bytes in
	file.ReadAt(data, off)
	buf := bytes.NewBuffer(data)

	heapPage := newHeapPage(&f.Desc, pageNo, f)
	heapPage.initFromBuffer(buf)
	var p Page = heapPage
	return &p, nil
}

// Add the tuple to the HeapFile.  This method should search through pages in
// the heap file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile.  We will
// add support for concurrent modifications in lab 3.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	if f.currPages == 0 {
		// Special case: Create the first page (Page 0) if it doesn't exist yet.
		f.currPages += 1
		newHP := *newHeapPage(&f.Desc, 0, f)
		_, err := newHP.insertTuple(t)
		if err != nil {
			return err
		}
		var p Page = &newHP

		// Insert page to file and bufpool
		f.flushPage(&p)
		_, err = f.bufPool.GetPage(f, 0, tid, ReadPerm)
		if err != nil {
			return err
		}
		return nil
	}
	inserted := false
	// pages are 0-indexed
	// Go through cached pages first and check if we can insert tuple
	for hh, p := range f.bufPool.Pages {
		if hh.FileName == f.Filename {
			rid, err := p.(*heapPage).insertTuple(t)
			if err != nil {
				// current page is full - check next page
				continue
			}
			if rid != nil {
				p.(*heapPage).Dirty = true
				// update curr page if needed
				inserted = true
				return nil
			}
		}
	}

	// TODO - check OTHER pages that are within currPages and NOT in buffer pool
	// Otherwise, try to add a new page
	if !inserted {
		// Must add new page
		f.currPages += 1
		// pages are 0-indexed
		newHP := *newHeapPage(&f.Desc, f.currPages-1, f)
		_, err := newHP.insertTuple(t)
		if err != nil {
			return err
		}
		var p Page = &newHP
		f.flushPage(&p)
		// Try to add page to buffer pool
		_, err = f.bufPool.GetPage(f, f.currPages-1, tid, ReadPerm)
		if err != nil {
			return err
		}
	}
	return nil

}

// Remove the provided tuple from the HeapFile.  This method should use the
// [Tuple.Rid] field of t to determine which tuple to remove.
// This method is only called with tuples that are read from storage via the
// [Iterator] method, so you can so you can supply the value of the Rid
// for tuples as they are read via [Iterator].  Note that Rid is an empty interface,
// so you can supply any object you wish.  You will likely want to identify the
// heap page and slot within the page that the tuple came from.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// Check if t.Rid is a slice of int
	rid, ok := t.Rid.(rID)
	if !ok {
		return fmt.Errorf("tuple's record ID is not a valid rID")
	}

	pageNum := rid.Page

	pg, err := f.bufPool.GetPage(f, pageNum, tid, WritePerm)
	if err != nil {
		return err
	}

	hPg := (*pg).(*heapPage)
	err = hPg.deleteTuple(t.Rid)
	if err != nil {
		return err
	}
	hPg.Dirty = true
	return nil
}

// Method to force the specified page back to the backing file at the appropriate
// location.  This will be called by BufferPool when it wants to evict a page.
// The Page object should store information about its offset on disk (e.g.,
// that it is the ith page in the heap file), so you can determine where to write it
// back.
func (f *HeapFile) flushPage(p *Page) error {
	hPg := (*p).(*heapPage)
	pageNo := hPg.PageNo

	hPg.Dirty = false
	pgData, _ := hPg.toBuffer()
	dataBytes := pgData.Bytes()

	// Open backing file and write page data to it at appropriate offset
	file, err := os.OpenFile(f.Filename, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		// file couldn't be opened or created
		return err
	}
	defer file.Close()
	_, err = file.WriteAt(dataBytes, int64(PageSize*pageNo))
	if err != nil {
		return err
	}

	return nil
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	return f.Desc.copy()

}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
func (f *HeapFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	i := 0
	slotNum := 0
	var pageIter func() (*Tuple, error)

	return func() (*Tuple, error) {
		for {
			var next *Tuple
			if pageIter != nil {
				next, err := pageIter()
				if err != nil {
					i += 1
					// Try to get page from bufpool: if error, return nil
					if i > f.currPages && f.currPages != 0 {
						return nil, nil
					} else {
						page, err := f.bufPool.GetPage(f, i, tid, ReadPerm)
						if err != nil {
							return nil, err
						}
						slotNum = 0
						pageIter = (*page).(*heapPage).tupleIter()
					}
				}
				if next == nil {
					continue
				}
				if next.Rid == nil {
					rid := rID{Page: i, Slot: slotNum}
					next.Rid = rid
				}
				slotNum += 1
				return next, err
			}
			if pageIter == nil || next == nil {
				if i > f.currPages && f.currPages != 0 {
					return nil, nil
				} else {
					page, err := f.bufPool.GetPage(f, i, tid, ReadPerm)
					if err != nil {
						return nil, err
					}
					slotNum = 0
					pageIter = (*page).(*heapPage).tupleIter()
				}
			}
		}
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	return heapHash{FileName: f.Filename, PageNo: pgNo}

}
