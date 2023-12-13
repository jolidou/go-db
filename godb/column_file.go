package godb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
)

// ColumnFile is an unordered collection of tuples Internally, it is arranged as a
// set of columnPage objects
//
// ColumnFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type ColumnFile struct {
	Filename    string
	Desc        TupleDesc
	bufPool     *ColumnBufferPool
	Columns     map[string][]int
	ColumnPages map[int]*FieldType // maps page no to field/column
	currPages   int
}

// Create a ColumnFile.
// Parameters
// - fromFile: backing file for the ColumnFile.  May be empty or a previously created column file.
// - td: the TupleDesc for the ColumnFile.
// - bp: the BufferPool that is used to store pages read from the ColumnFile
//
// May return an error if the file cannot be opened or created.
func NewColumnFile(fromFile string, td *TupleDesc, bp *ColumnBufferPool) (*ColumnFile, error) {
	var file *os.File
	file, err := os.Open(fromFile)
	defer file.Close()
	if err != nil {
		file, err = os.Create(fromFile)
		if err != nil {
			return nil, err
		}
	}
	columns := make(map[string][]int)
	pages := make(map[int]*FieldType)

	return &ColumnFile{Filename: fromFile, Desc: *td, bufPool: bp, Columns: columns, ColumnPages: pages}, nil
}

// Read the specified page number from the ColumnFile on disk.  This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to the
// appropriate offset, read the bytes in, and construct a [columnPage] object, using
// the [columnPage.initFromBuffer] method.
func (f *ColumnFile) readPage(pageNo int) (*Page, error) {
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
	columnFt := f.ColumnPages[pageNo]
	// fmt.Println("column ft", columnFt)

	cp := newColumnPage(columnFt, pageNo, f)
	cp.initFromBuffer(buf)
	var p Page = cp
	return &p, nil
}

// Add the tuple to the ColumnFile.  This method should search through pages in
// the column file, looking for empty slots and adding the tuple in the first
// empty slot if finds.
//
// If none are found, it should create a new [columnPage] and insert the tuple
// there, and write the columnPage to the end of the ColumnFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or ColumnFile.  We will
// add support for concurrent modifications in lab 3.
func (f *ColumnFile) insertTuple(t *Tuple, tid TransactionID) error {
	for i, field := range t.Desc.Fields {
		newt := new(Tuple)
		newt.Desc.Fields = []FieldType{t.Desc.Fields[i]}
		newt.Fields = []DBValue{t.Fields[i]}

		if f.currPages == 0 {
			// Special case: Create the pages if it doesn't exist yet.
			newCP := *newColumnPage(&field, 0, f)
			f.currPages += 1
			var p Page = &newCP
			// Insert page to file and bufpool
			f.flushPage(&p)

			TP, err := f.bufPool.GetPage(f, 0, tid, WritePerm)

			if err != nil {
				return err
			}

			CP := (*TP).(*columnPage)

			rid, err := CP.insertTuple(newt)
			if rid == nil {
				return errors.New("nil RID")
			}
			var old_pages []int
			var old_slots []int
			if t.Rid != nil {
				old_pages = t.Rid.(rrID).Page
				old_slots = t.Rid.(rrID).Slot
			}

			t.Rid = rrID{Page: append(old_pages, rid.(rID).Page), Slot: append(old_slots, rid.(rID).Slot)}
			// fmt.Println("Case 1", rid, t)
			// set ColumnPages page number to field/column
			f.ColumnPages[f.currPages] = &field
			var colPgList []int
			colPgList = append(colPgList, 0)
			// Record page as part of column's set
			f.Columns[field.Fname] = colPgList
			if err != nil {
				return err
			}
			CP.setDirty(true)
			continue
		}

		inserted := false
		// pages are 0-indexed
		// Go through cached pages first and check if we can insert tuple
		for hh, _ := range f.bufPool.Pages {
			// check if columnpage is the same field
			if hh.FileName == f.Filename && f.ColumnPages[hh.PageNo].fieldEquals(field) {

				p, err := f.bufPool.GetPage(f, hh.PageNo, tid, WritePerm)

				if err != nil {
					fmt.Println("erroring", err)
					return err
				}

				cp := (*p).(*columnPage)
				rid, err := cp.insertTuple(newt)
				var old_pages []int
				var old_slots []int
				if t.Rid != nil {
					old_pages = t.Rid.(rrID).Page
					old_slots = t.Rid.(rrID).Slot
				}
				t.Rid = rrID{Page: append(old_pages, rid.(rID).Page), Slot: append(old_slots, rid.(rID).Slot)}

				// fmt.Println("Case 2", rid, t)
				if err != nil {
					// current page is full - FLUSH and check next page
					var p Page = cp
					f.flushPage(&p)
					continue
				}
				if rid == nil {
					return errors.New("nil RID 2")
				}
				if rid != nil {
					// update curr page if needed
					inserted = true
					cp.setDirty(true)
					currList := f.Columns[field.Fname]
					if !slices.Contains(currList, hh.PageNo) {
						currList = append(currList, hh.PageNo)
						f.Columns[field.Fname] = currList
					}
					if err != nil {
						return err
					}
				}
				break
			}

		}

		// TODO - check OTHER pages that are within currPages and NOT in buffer pool
		// Otherwise, try to add a new page
		if !inserted {
			// Check other pages within currPages that are NOT in buffer pool
			pgsToCheck := f.Columns[field.Fname]
			for _, pgNo := range pgsToCheck {
				TP, err := f.bufPool.GetPage(f, pgNo, tid, WritePerm)
				if err != nil {
					return err
				}
				cp := (*TP).(*columnPage)
				rid, err := cp.insertTuple(newt)
				if err != nil {
					return err
				}
				if rid == nil {
					continue
				}
				var old_pages []int
				var old_slots []int
				if t.Rid != nil {
					old_pages = t.Rid.(rrID).Page
					old_slots = t.Rid.(rrID).Slot
				}
				t.Rid = rrID{Page: append(old_pages, rid.(rID).Page), Slot: append(old_slots, rid.(rID).Slot)}
				cp.setDirty(true)
				inserted = true
				if err != nil {
					return err
				}
				// insert in first available column page that matches field type
			}
			if !inserted {
				// Must add new page
				f.currPages += 1
				// pages are 0-indexed
				newCP := *newColumnPage(&field, f.currPages-1, f)
				var p Page = &newCP
				f.flushPage(&p)
				TP, err := f.bufPool.GetPage(f, f.currPages-1, tid, WritePerm)
				if err != nil {
					return err
				}

				CP := (*TP).(*columnPage)
				rid, err := CP.insertTuple(newt)
				var old_pages []int
				var old_slots []int
				if t.Rid != nil {
					old_pages = t.Rid.(rrID).Page
					old_slots = t.Rid.(rrID).Slot
				}
				t.Rid = rrID{Page: append(old_pages, rid.(rID).Page), Slot: append(old_slots, rid.(rID).Slot)}
				// fmt.Println("Case 3", rid, t)
				if rid == nil {
					return errors.New("nil RID 3")
				}

				// set ColumnPages page number to field/column
				f.ColumnPages[f.currPages-1] = &field

				// Record page as part of column's set of pages
				currList := f.Columns[field.Fname]
				if !slices.Contains(currList, f.currPages-1) {
					currList = append(currList, f.currPages-1)
					f.Columns[field.Fname] = currList
				}
				if err != nil {
					return err
				}

				if err != nil {
					fmt.Println("error here", err)
					return err
				}
				CP.setDirty(true)
			}
		}
	}
	return nil
}

func (f *ColumnFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// Check if t.Rid is an Rid
	rid, ok := t.Rid.(rID)
	if !ok {
		ridd, ok := t.Rid.(rrID)
		if !ok {
			return fmt.Errorf("tuple's record ID is not a valid rID")
		}
		for i, _ := range ridd.Page {
			pageNum := ridd.Page[i]
			pg, err := f.bufPool.GetPage(f, pageNum, tid, WritePerm)

			if err != nil {
				return err
			}

			hPg := (*pg).(*columnPage)
			delete_rid := rID{Page: ridd.Page[i], Slot: ridd.Slot[i]}
			err = hPg.deleteTuple(delete_rid)
			if err != nil {
				return err
			}
			hPg.Dirty = true
		}
		return nil

	}

	pageNum := rid.Page
	pg, err := f.bufPool.GetPage(f, pageNum, tid, WritePerm)

	if err != nil {
		return err
	}

	hPg := (*pg).(*columnPage)
	err = hPg.deleteTuple(t.Rid)
	if err != nil {
		return err
	}
	hPg.Dirty = true
	return nil
}

func (f *ColumnFile) flushPage(p *Page) error {
	cPg := (*p).(*columnPage)
	pageNo := cPg.PageNo
	f.ColumnPages[pageNo] = cPg.Field

	cPg.Dirty = false
	pgData, _ := cPg.toBuffer()
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

// [Operator] descriptor method -- return the TupleDesc for this ColumnFile
// Supplied as argument to NewColumnFile.
func (f *ColumnFile) Descriptor() *TupleDesc {
	return f.Desc.copy()

}

// Return the number of pages in the column file
func (f *ColumnFile) NumPages() int {
	// Stat the file
	file, _ := os.Open(f.Filename)
	defer file.Close()

	fileInfo, _ := file.Stat()

	// Get the file size in bytes
	fileSize := fileInfo.Size()

	numPages := int(fileSize) / PageSize

	return int(numPages)
}

// Modify to build ColumnFile instead of HeapFile
// --
// Load the contents of a column file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
func (f *ColumnFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
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

		tid := NewTID()
		bp := f.bufPool
		bp.BeginTransaction(tid)
		for fno, field := range fields {
			currField := f.Descriptor().Fields[fno]
			newFt := FieldType{currField.Fname, currField.TableQualifier, currField.Ftype}
			var ft []FieldType
			ft = append(ft, newFt)
			newDescriptor := TupleDesc{ft}
			tid := NewTID()
			switch currField.Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int64(floatVal)
				// Try to insert single-column tuple into corresponding column of the file
				var newValue []DBValue
				newValue = append(newValue, IntField{intValue})
				newT := Tuple{newDescriptor, newValue, nil}
				err = f.insertTuple(&newT, tid)
				if err != nil {
					return err
				}
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				var newValue []DBValue
				newValue = append(newValue, StringField{field})
				newT := Tuple{newDescriptor, newValue, nil}
				err := f.insertTuple(&newT, tid)
				if err != nil {
					return err
				}
			}
		}

		// hack to force dirty pages to disk
		// for j := 0; j < f.currPages; j++ {
		// 	fmt.Println("GET PG:", j )
		// 	pg, err := bp.GetPage(f, j, tid, ReadPerm)
		// 	if pg == nil || err != nil {
		// 		fmt.Println("page nil or error", err)
		// 		break
		// 	}
		// 	if (*pg).isDirty() {
		// 		fmt.Println("IS DIRTY:", j)
		// 		(*f).flushPage(pg)
		// 		(*pg).setDirty(false)
		// 	}

		// }

		//commit frequently, to avoid all pages in BP being full
		bp.CommitTransaction(tid)
	}
	return nil
}

// [Operator] COLUMN-specific iterator method
// Return a function that iterates through the records in the column file.
// Accepts a list of columns to read and returns tuples that contain only records from these columns.
func (f *ColumnFile) ColumnIterator(toRead []FieldType, tid TransactionID) (func() (*Tuple, error), error) {
	i := 0
	slotNum := 0
	var pageIter func() (*Tuple, error)
	// find page numbers of interest (for the columns in toRead)
	var pagesToCheck []int
	// BUILD TUPLES:
	currTups := make(map[int]Tuple) // map tuple # to current tuple (build it up)
	// COUNTER FOR WHICH TUPLE EACH COLUMN IS AT
	tupCounts := make(map[string]int) // map column name to tuple count thus far
	totalTups := 0

	for _, ft := range toRead {
		tupCounts[ft.Fname] = 0 // initialize tuple counts: all 0 thus far
		for _, pgNo := range f.Columns[ft.Fname] {
			if !slices.Contains(pagesToCheck, pgNo) {
				pagesToCheck = append(pagesToCheck, pgNo)
				fmt.Println("CHECK:", pgNo, "for", ft.Fname)
			}
		}
	}

	return func() (*Tuple, error) {
		for {
			var next *Tuple
			if i > f.currPages && f.currPages != 0 {
				return nil, nil
			}
			if !slices.Contains(pagesToCheck, i) {
				i += 1
				continue
			}
			// Check if there is an existing page iterator
			if pageIter != nil {
				next, err := pageIter()
				fmt.Println(i, slotNum, next)
				if i >= f.currPages && f.currPages != 0 {
					return nil, nil
				}

				// or if page number (i) not of interest (doesn't have any column of interest)
				if next == nil {
					i += 1

					// Try to get the page from the bufpool: if an error, return nil
					if i > f.currPages && f.currPages != 0 {
						return nil, nil
					} else if !slices.Contains(pagesToCheck, i) {
						pageIter = nil
						slotNum = 0
						continue
					} else {
						page, err := f.bufPool.GetPage(f, i, tid, ReadPerm)
						if page == nil {
							// fmt.Println("C2", i, slotNum, tid)
							return nil, nil
						}
						if err != nil {
							// fmt.Println("C3", i, slotNum, tid)
							return nil, err
						}
						slotNum = 0
						pageIter = (*page).(*columnPage).tupleIter()
					}
				}

				// Check if the RID is nil and set it
				if next != nil && next.Rid == nil {
					rid := rID{Page: i, Slot: slotNum}
					next.Rid = rid
				}

				if err != nil {
					return nil, err
				}

				if next != nil {
					slotNum += 1

					// build tuple and return if complete
					tupNumber := tupCounts[next.Desc.Fields[0].Fname]
					currTup, exists := currTups[tupNumber]
					if exists {
						currTup.Desc.Fields = append(currTup.Desc.Fields, next.Desc.Fields...)
						currTup.Fields = append(currTup.Fields, next.Fields...)
						// check if the tuple is complete (all necessary fields found)
						// if so, return the complete tuple with RID = tuple # overall
						if len(currTup.Desc.Fields) == len(toRead) {
							totalTups += 1
							currTup.Rid = totalTups
							currTups[tupNumber] = currTup
							tupCounts[next.Desc.Fields[0].Fname] += 1
							return &currTup, nil
						}
						currTups[tupNumber] = currTup
						tupCounts[next.Desc.Fields[0].Fname] += 1 // one tuple added
					} else {
						tdCopy := next.Desc.copy()
						fields := next.Fields[:]
						currTups[tupNumber] = Tuple{Desc: *tdCopy, Fields: fields}
						tupCounts[next.Desc.Fields[0].Fname] += 1 // one tuple added
					}
					continue
				}

				if pageIter == nil {
					return nil, nil
				}

				return nil, nil
			}

			if pageIter == nil || next == nil {
				// Check if the page number is out of bounds
				if i > f.currPages && f.currPages != 0 {
					return nil, nil
				} else if !slices.Contains(pagesToCheck, i) {
					pageIter = nil
					slotNum = 0
					continue
				} else {
					page, err := f.bufPool.GetPage(f, i, tid, ReadPerm)
					if err != nil {
						return nil, err
					}
					slotNum = 0

					// Set the page iterator for the new page
					pageIter = (*page).(*columnPage).tupleIter()
				}
			}
		}
	}, nil
}

// [Operator] Generic iterator method (included so column file still implements DBFile interface)
// Return a function that iterates through the records in the column file.
// Accepts a list of columns to read and returns tuples that contain only records from these columns.
func (f *ColumnFile) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	i := 0
	slotNum := 0
	var pageIter func() (*Tuple, error)

	return func() (*Tuple, error) {
		for {
			var next *Tuple
			if pageIter != nil {
				next, err := pageIter()
				if i >= f.currPages && f.currPages != 0 {
					// fmt.Println("C0", i, slotNum)
					return nil, nil
				}
				if next == nil {
					i += 1
					// Try to get page from bufpool: if error, return nil
					if i > f.currPages && f.currPages != 0 {
						// fmt.Println("C1", i, slotNum, tid)
						return nil, nil
					} else {
						page, err := f.bufPool.GetPage(f, i, tid, ReadPerm)
						if page == nil {
							// fmt.Println("C2", i, slotNum, tid)
							return nil, nil
						}
						if err != nil {
							// fmt.Println("C3", i, slotNum, tid)
							return nil, err
						}
						// fmt.Println("NEW PAGE ITER")
						pageIter = (*page).(*columnPage).tupleIter()
						next, err := pageIter()
						if next == nil {
							// fmt.Println("HERE")
							return nil, nil
						}
						slotNum = 1
						fmt.Println(next)
						return next, nil
					}
				}
				if next != nil && next.Rid == nil {
					rid := rID{Page: i, Slot: slotNum}
					next.Rid = rid
				}
				if err != nil {
					// fmt.Println("C4", i, slotNum, tid)
					return nil, err
				}
				slotNum += 1
				if next != nil {
					return next, nil
				}
				if pageIter == nil {
					return nil, nil
				}
				continue
			}
			if pageIter == nil || next == nil {
				if i > f.currPages && f.currPages != 0 {
					// fmt.Println("C5", i, slotNum, tid)
					return nil, nil
				} else {
					page, err := f.bufPool.GetPage(f, i, tid, ReadPerm)
					if err != nil {
						// fmt.Println("C6", i, slotNum, tid)
						return nil, err
					}
					slotNum = 0
					// fmt.Println("NEW PAGE ITER 2")
					pageIter = (*page).(*columnPage).tupleIter()
				}
			}
		}
	}, nil
}

// func (f *ColumnFile) readPage(pageNo int) (*Page, error) {

// }

//methods used by buffer pool to manage retrieval of pages
// readPage(pageNo int) (*Page, error)
// flushPage(page *Page) error
// pageKey(pgNo int) any //uint64

// Operator

// internal strucuture to use as key for a column page
type columnHeapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *ColumnFile) pageKey(pgNo int) any {
	return heapHash{FileName: f.Filename, PageNo: pgNo}

}
