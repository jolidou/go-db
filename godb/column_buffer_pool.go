package godb

import (
	"errors"
	"fmt"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
// type RWPerm int

// const (
// 	ReadPerm  RWPerm = iota
// 	WritePerm RWPerm = iota
// )

type ColumnBufferPool struct {
	Pages    map[heapHash]Page
	NumPages int
}

// Create a new BufferPool with the specified number of pages
func NewColumnBufferPool(numPages int) *ColumnBufferPool {
	pgs := make(map[heapHash]Page)
	return &ColumnBufferPool{Pages: pgs, NumPages: numPages}
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
func (bp *ColumnBufferPool) FlushAllPages() {
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
func (bp *ColumnBufferPool) AbortTransaction(tid TransactionID) {
	return
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *ColumnBufferPool) CommitTransaction(tid TransactionID) {
	return
}

func (bp *ColumnBufferPool) BeginTransaction(tid TransactionID) error {
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
func (bp *ColumnBufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (*Page, error) {
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
		fmt.Println("couldn't read page", pageNo)
		return nil, err
	}

	// Evict pages if necessary
	if bp.NumPages == len(bp.Pages) {
		// Find the first non-dirty page associated with the ongoing transaction
		var pgToFlush Page
		foundClean := false
		for key, p := range bp.Pages {
			// Check if the page belongs to the ongoing transaction
			if !p.isDirty() {
				pgToFlush = p
				foundClean = true
				delete(bp.Pages, key)
				break
			}
		}

		// If no clean pages were found for eviction, return an error
		if !foundClean {
			return nil, errors.New("buffer pool is full, and no clean pages found for eviction")
		}

		// Flush the evicted page to disk if it's dirty
		if pgToFlush.isDirty() {
			f := pgToFlush.getFile()
			file := *f
			file.flushPage(&pgToFlush)
			pgToFlush.setDirty(false)
		}
	}

	// Add the newly retrieved page to the buffer pool
	bp.Pages[pageKey] = *pgOutput
	// Output page
	return pgOutput, nil
}