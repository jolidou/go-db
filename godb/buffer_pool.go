package godb

import (
	"errors"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type BufferPool struct {
	Pages map[heapHash]Page
	NumPages int
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	pgs := make(map[heapHash]Page)
	return &BufferPool{Pages: pgs, NumPages: numPages}
}

// Testing method -- iterate through all pages in the buffer pool
// and call [DBFile.flushPage] on them. Does not need to be thread/transaction safe
func (bp *BufferPool) FlushAllPages() {
	for _, page := range bp.Pages {
		f := page.getFile()
		file := *f
		file.flushPage(&page)
		page.setDirty(false)
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtired will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	return nil
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. For lab 1, you do not need to
// implement locking or deadlock detection. [For future labs, before returning the page,
// attempt to lock it with the specified permission. If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock]. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
	// Check if page is in bp - if so, retrieve it
	pageKey := file.pageKey(pageNo).(heapHash)
	if pg, pgKeyExists := bp.Pages[pageKey]; pgKeyExists {
		if pg != nil {
			return &pg, nil
		}
	}

	// Otherwise, retrieve specified page from specified DBFile
	pgOutput, err := file.readPage(pageNo)
	if err != nil {
		return nil, err
	}

	//tups := (*pgOutput).(*heapPage).Tuples

	// Reconstruct tuple RIDs
	// for i, t := range tups {
	// 	if t != nil {
	// 		var rid []int
	// 		// RID: [pageNum, slotNum]
	// 		rid = append(rid, pageNo, i)
	// 		t.Rid = rid
	// 	}
	// }

	// Filename:
	currFile := file.(*HeapFile).Filename

	// Evict pages if necessary
	if bp.NumPages == len(bp.Pages) {
		// assume smallest (earliest) page number is highest possible page index in file - might not be in buffer pool
		earliest := file.(*HeapFile).currPages
		// curr pages 
		if earliest == 0 {
			earliest += 1
		}
		var pgToFlush Page
		foundClean := false
		for key, p := range bp.Pages {
			// if !p.isDirty() {
				// TODO - Delete first non-dirty page
				if key.PageNo <= earliest && key.FileName == currFile {
					pgToFlush = p
					foundClean = true
					earliest = key.PageNo
				}
				// foundClean = true // TODO - Use to determine if a non-dirty page exists
				//break
			// }
		}
		if !foundClean {
			return nil, errors.New("buffer pool is full, and no clean pages found for eviction")
		}

		if foundClean {
			// Delete earliest page in buf pool
			err = file.(*HeapFile).flushPage(&pgToFlush)
			if err != nil {
				return nil, err
			}
			delete(bp.Pages, heapHash{FileName: currFile, PageNo: earliest})
		}
	}

	// Add the newly retrieved page to the buffer pool
	bp.Pages[pageKey] = *pgOutput
	
	// Output page
	return pgOutput, nil
}