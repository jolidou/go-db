package godb

import (
	"errors"
	"sync"
	"time"
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

// used to keep track of what pages the tid have locks on. So page is the page, perm is either ReadPerm or WritePerm
// depending on what type of lock was needed on that page, file is the page's file and pageNo is the page's pageno
type TransactionTuple struct {
	page   Page
	perm   RWPerm
	file   DBFile
	pageNo int
}

type BufferPool struct {
	Pages    map[heapHash]Page
	NumPages int
	Locks    map[TransactionID][]TransactionTuple // this maintains a map from the tid to any page-level locks the transaction TID has
	// Waitlist: Wait-for dependency graph - map of maps from TID to TIDs of transactions it is waiting for
	// Waitlist is a map of maps to enable O(1) delete without iterating through a list whenever we want to
	// remove a transaction ID
	Waitlist map[TransactionID]map[TransactionID]bool
	mutex    sync.Mutex
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) *BufferPool {
	pgs := make(map[heapHash]Page)
	locks := make(map[TransactionID][]TransactionTuple)
	waitlist := make(map[TransactionID]map[TransactionID]bool)
	return &BufferPool{Pages: pgs, NumPages: numPages, Locks: locks, Waitlist: waitlist}
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe
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
	bp.mutex.Lock()
	defer bp.mutex.Unlock()

	// revert changes made by transaction by discarding page in memory
	for _, tt := range bp.Locks[tid] {
		if tt.perm == WritePerm {
			tt.page.setDirty(false) // set as clean??
			pageKey := tt.file.pageKey(tt.pageNo).(heapHash)
			delete(bp.Pages, pageKey)
		}

	}
	// let go of all locks
	bp.Locks[tid] = []TransactionTuple{}
	time.Sleep(11000)
	// Remove TID from adjacency list:
	// go through all adj lists (maps) in Waitlist values and delete TID (is ok too if it's not present)
	for _, adjList := range bp.Waitlist {
		delete(adjList, tid)
	}
	// delete transaction at TID's adj list
	delete(bp.Waitlist, tid)

}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	// flush dirty pages to disk
	for _, tt := range bp.Locks[tid] {
		if tt.page.isDirty() {
			tt.file.(*HeapFile).flushPage(&tt.page)
		}
	}
	bp.Locks[tid] = []TransactionTuple{}
	// Remove tid from everywhere in adjancency list
	for _, adjList := range bp.Waitlist {
		delete(adjList, tid)
	}

	// delete transaction at TID's adj list
	delete(bp.Waitlist, tid)
}

func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	bp.Locks[tid] = []TransactionTuple{}
	newAdjacency := make(map[TransactionID]bool)
	bp.Waitlist[tid] = newAdjacency
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
	// acquire mutex
	bp.mutex.Lock()
	defer bp.mutex.Unlock()
	// Check if page is in bp - if so, retrieve it
	pageKey := file.pageKey(pageNo).(heapHash)
	if pg, pgKeyExists := bp.Pages[pageKey]; pgKeyExists {
		// write perm (exclusive)
		if perm == WritePerm {
			//while page locked by another transaction
			for bp.pageLockedByAnotherTransaction(pg, tid) {
				// Check for cycle (run DFS) - abort if found
				if bp.detectCycle() {
					bp.mutex.Unlock()
					bp.AbortTransaction(tid)
					bp.mutex.Lock()
					return nil, errors.New("need to abort")
				}
				bp.mutex.Unlock()
				time.Sleep(10000) // block for 5 milliseconds
				bp.mutex.Lock()
			}
			// lock can be acquired, so acquire lock and mark in data stucture
			bp.Locks[tid] = append(bp.Locks[tid], TransactionTuple{pg, perm, file, pageNo})

		} else { // read perm (shared)
			for bp.pageLockedByExclusiveTransaction(pg, tid) {
				// Check for cycle (run DFS) - abort if found
				if bp.detectCycle() {
					bp.mutex.Unlock()
					bp.AbortTransaction(tid)
					bp.mutex.Lock()
					return nil, errors.New("need to abort")
				}
				bp.mutex.Unlock()
				time.Sleep(10000)
				bp.mutex.Lock()
			}
			bp.Locks[tid] = append(bp.Locks[tid], TransactionTuple{pg, perm, file, pageNo})
		}

		if pg != nil {
			return &pg, nil
		}
	}

	// Otherwise, retrieve specified page from specified DBFile
	pgOutput, err := file.readPage(pageNo)
	if err != nil {
		return nil, err
	}

	// Filename:
	//currFile := file.(*HeapFile).Filename
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
			if !p.isDirty() {
				// Delete first non-dirty page
				if key.PageNo <= earliest {
					pgToFlush = p
					foundClean = true
					earliest = key.PageNo
					delete(bp.Pages, key)
					pgToFlush.setDirty(false)
					break
				}
			}
		}
		if !foundClean {
			return nil, errors.New("buffer pool is full, and no clean pages found for eviction")
		}
	}

	// Add the newly retrieved page to the buffer pool
	bp.Pages[pageKey] = *pgOutput
	// write perm (exclusive)
	if perm == WritePerm {
		//while page locked by another transaction
		for bp.pageLockedByAnotherTransaction(*pgOutput, tid) {
			if bp.detectCycle() {
				bp.mutex.Unlock()
				bp.AbortTransaction(tid)
				bp.mutex.Lock()
				return nil, errors.New("need to abort")
			}
			bp.mutex.Unlock()
			time.Sleep(10000) // block for 5 milliseconds
			bp.mutex.Lock()
		}
		// lock can be acquired, so acquire lock and mark in data stucture
		bp.Locks[tid] = append(bp.Locks[tid], TransactionTuple{*pgOutput, perm, file, pageNo})

	} else { // read perm (shared)
		for bp.pageLockedByExclusiveTransaction(*pgOutput, tid) {
			if bp.detectCycle() {
				bp.mutex.Unlock()
				bp.AbortTransaction(tid)
				bp.mutex.Lock()
				return nil, errors.New("need to abort")
			}
			bp.mutex.Unlock()
			time.Sleep(10000)
			bp.mutex.Lock()
		}
		bp.Locks[tid] = append(bp.Locks[tid], TransactionTuple{*pgOutput, perm, file, pageNo})
	}
	//bp.mutex.Unlock()
	// Output page
	return pgOutput, nil
}

func (bp *BufferPool) pageLockedByAnotherTransaction(page Page, t_id TransactionID) bool {
	var foundConflict = false
	// Check if the page is locked by another transaction.
	for tid, value := range bp.Locks {
		for _, tt := range value {
			if tt.page == page && tid != t_id {
				// add tid to t_id's adjacency list
				_, exists := bp.Waitlist[t_id]
				if exists {
					bp.Waitlist[t_id][tid] = true
					foundConflict = true
				}

			}
		}
	}

	return foundConflict

}

func (bp *BufferPool) pageLockedByExclusiveTransaction(page Page, t_id TransactionID) bool {
	var foundConflict = false
	// Check if the page is locked by another transaction.
	for tid, value := range bp.Locks {
		for _, tt := range value {
			if tt.page == page && tt.perm == WritePerm && tid != t_id {
				// add tid to t_id's adjacency list
				_, exists := bp.Waitlist[t_id]
				if exists {
					bp.Waitlist[t_id][tid] = true
					foundConflict = true
				}
			}
		}
	}
	return foundConflict
}


// Helper method: Detect cycle in subgraph reachable from vertex tid
func (bp *BufferPool) detectCycleHelper(tid TransactionID, parent TransactionID, visited map[TransactionID]bool) bool {
	// mark tid as visited
	visited[tid] = true

	// go through neighbors- if not visited, then recurse on it
	for neighbor, _ := range bp.Waitlist[tid] {
		// if node isn't visited, recurse on it
		if !visited[neighbor] {
			if bp.detectCycleHelper(neighbor, tid, visited) {
				return true
			}
			// if adjacent vertex is visited and parent of current, then there is cycle
		} else if parent == neighbor {
			return true
		}
	}

	return false
}

// Implement DFS: Return true if graph contains cycle, else false
func (bp *BufferPool) detectCycle() bool {
	visited := make(map[TransactionID]bool)

	// mark all vertices as not visited
	for v, _ := range bp.Waitlist {
		visited[v] = false
	}

	var dummyTid TransactionID

	// call recursive helper to detect cycle
	for u, _ := range visited {
		// don't recur if already visited
		if !visited[u] {
			if bp.detectCycleHelper(u, dummyTid, visited) {
				return true
			}
		}
	}
	return false
}
