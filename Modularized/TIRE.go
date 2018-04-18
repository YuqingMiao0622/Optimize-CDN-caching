package ObjectBased

import (
	"container/list"
)

var(
	/* TIRE */
	ghostCache   *GhostCache
	quota        int64			// quota for each quantum
	quantum      int			// 5 min --> 1 million requests
	K            int			// slack variable
	intervals    []int
	threshold    int
	currInterval int
)

/**
	Set up the ghost cache.
 */
func GhoseCacheSetUp(size int64) {
	ghostCache = &GhostCache{
		accessCount: 	make(map[string]int),
		currSize: 		0,
		maxSize: 		size / 8,
		queue:			list.New(),
		objQueueMap: 	make(map[string]*list.Element),
	}
}

func TireSetUp(interval int, k int, bal int64, q int64, quan int) {
	K = k
	intervals = make([]int, 0)
	intervals = append(intervals, 1)
	base := (K - 1) / interval
	for n := 1; n <= interval; n++ {
		intervals = append(intervals, 1 + n * base)
	}
	DFmtPrintf("TireSetUp:: intervals: %v.\n", intervals)
	balance = bal
	quota = q
	quantum = quan
	E = 0
	threshold = 0		// admit everything at the beginning
	currInterval = 1
}

/**
	When one quantum finishes, need to calculate balance to determine whether this quantum is allowed to cache some objects
	Besides, reset the erasure bytes (E) and current interval (to 1)
 */
func updateTire() {
	if numRequest % Epoch == 0 {
		DFmtPrintf("\n")
		DFmtPrintf("updateTire:: Number of requests: %d, last quantum: interval: %d, written bytes: %d. ", numRequest, currInterval, E)
		balance += quota - E
		DFmtPrintf("Current balance: %d.\n", balance)
		if balance <= 0 {
			threshold = -1
			DFmtPrintf("updateTire:: Number of requests: %d. No insertion, wait until next quantum.\n", numRequest)
		} else {
			if currInterval <= intervals[1] {
				threshold = 0
			} else if currInterval <= intervals[len(intervals) - 2] {
				threshold = currInterval - 1
			} else {
				threshold = -1
			}
		}
		// reset erasure bytes and current interval
		E = 0
		currInterval = 1
	}
}

/**
	admission control using TIRE --> return whether this object can be admit or not
 */
func admissionControlTIRE(id string, size int64) bool {
	admit := false
	if threshold != -1 {
		// some objects are allowed to cache during this quantum --> check written bytes during this quantum
		if currInterval <= intervals[1] {
			admit = true
		} else {
			accCount, ok := ghostCache.accessCount[id]
			if ok {
				if accCount >= threshold {
					admit = true
				} else {
					admit = false
				}
				accCount++
			} else {
				accCount = 1
				admit = false
			}
			ghostCache.accessCount[id] = accCount // update access counter
		}

		// update current interval and threshold
		if admit {
			E += size
			if E > int64(currInterval) * quota {
				currInterval++
				threshold++
			}
		}
	}
	return admit
}

func warmUpTIRE(id string, size int64) bool {
	if numRequest / Epoch < 250 {
		return true
	}
	return admissionControlTIRE(id, size)
}

/**
	update the LRU list in ghost cache. When ghost cache is full, remove the object in LRU position of the queue.
	Then remove or add this object to the MRU position of the queue.
 */
func updateGhostQueue(id string, exist bool) {
	if ghostCache.currSize >= ghostCache.maxSize {
		delete(ghostCache.objQueueMap, ghostCache.queue.Front().Value.(*AccessCount).objectId)
		ghostCache.queue.Remove(ghostCache.queue.Front())
		ghostCache.currSize--
	}

	if exist {
		ghostCache.queue.Remove(ghostCache.objQueueMap[id])
		ghostCache.currSize--
	}
	ghostCache.queue.PushBack(&AccessCount{id})
	ghostCache.objQueueMap[id] = ghostCache.queue.Back()
	ghostCache.currSize++
	//DFmtPrintf("updateGhostQueue:: current queue size: %d.\n", ghostCache.queue.Len())
}