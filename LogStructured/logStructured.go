package LogStructured

import (
	"strconv"
	"container/list"
	"fmt"
)

/*

 */

//const maxBoxSize = 20
const maxBoxSize = 104857600		// 100 MB

type Object struct {
	objectId 	string
	objectSize	int64
}

type Box struct {
	boxId		int64					//
	//objects		[]*Object			// hold objects with corresponding object size. Not required in simulation
	currSize	int64					// record the current size of the box
	upperBound	int64					// the upper bound of object size this box can hold
	objOffsetMap map[string]int64		// map from object id to the offset where this object is stored.
}

type QueuePos struct {
	element *list.Element // position in the queue
	hot     bool          // in hot queue or not
}

var (
	hotQueue		*list.List		// holds box
	coldQueue		*list.List
	hotSize 		int64
	coldSize		int64
	maxCacheSize	int64
	boxQueueMap		map[int64]*QueuePos		// box id --> position in the hot queue or cold queue
	granularity		[]int64
	openBoxes		map[int64]*Box	// upper bound --> open boxes
	nextBoxId		int64				// record next box Id
	sealedBoxes		map[int64][]int64	// upper bound --> all sealed boxes with the corresponding upper bound

	// object id --> box id. For speeding up the code. Only the objects cached in the flash can be added into this map.
	// Similarly, if one box is evicted from cold queue, then the objects in that box need to be removed from the map.
	cachedObj		map[string]int64

	// experiment part
	frag			int64				// record fragmentation
	numSeal			int64				// number of sealed boxes
	numRequest		int64 				// number of request
	hits			int64				// number of hits
	hitBytes		int64
	reqBytes		int64
)

/**
	Set up flash cache.
	Set the maximum cache size and create "number" boxes and "granularity" is used
	to specify the size range (in Bytes) of objects each box is supposed to hold.
	For example, if number = 4, and granularity is {1024, 2048, 4096,100000}.
	Then four boxes are created, and they are supposed to hold objects 0 ~ 1024 Bytes,
	1024 ~ 2048 Bytes, 2048 ~ 4096 Bytes and 4096 ~ 100000 Bytes separately.
 */
func StartUp(cacheSize int64, number int, upperBounds []int64) {
	maxCacheSize = cacheSize / 2
	DDPrintf("StartUp:: Cache size is: %d, cold/hot queue size: %d.\n", cacheSize, maxCacheSize)
	hotQueue = list.New()
	coldQueue = list.New()
	hotSize = 0
	coldSize = 0
	nextBoxId = 1
	openBoxes = make(map[int64]*Box, number)
	granularity = upperBounds 			// shadow copy or deep copy?
	sealedBoxes = make(map[int64][]int64)
	boxQueueMap = make(map[int64]*QueuePos)
	for _, upperBound := range upperBounds {
		newBox := &Box {
			boxId:			nextBoxId,
			currSize: 		0,
			upperBound:		upperBound,
			objOffsetMap:	make(map[string]int64),
		}
		nextBoxId++
		openBoxes[upperBound] = newBox
	}

	// experiment part
	frag = 0
	numSeal = 0
	numRequest = 0
	hits = 0

	// advanced
	hitBytes = 0
	reqBytes = 0
	cachedObj = make(map[string]int64)
	fragRatio = 0

	// new graph
	HitRatioTime = make([]float64, 0)
	SealedBoxRatioTime = make([]float64, 0)
	SealedBoxNumber = make([]int64, 0)
}

/**
	When a new object comes in, check whether it is cached or not. If it's cached, then update the LRU list
 */
func NewRequest(id string, size string) {
	// In flash level: size --> box indexes in flash. In box level: object id --> whether the object is in cache or not
	numRequest++
	//fmt.Printf("New request with object id: %s and size: %s. Total requests: %d\n", id, size, numRequest)
	DPrintf("New request with object id: %s and size: %s. Total requests: %d\n", id, size, numRequest)
	object, err := strconv.Atoi(size)
	objectSize := int64(object)
	if err != nil {
		DPrintf("Input size %s cannot be converted to int64 type with error %s.\n", size, err)
	}
	bound := getBound(objectSize)
	DPrintf("%s should be put into open box with upper bound %d.\n", id, bound)
	if bound == -1 {
		DPrintf("Object size %s exceeds the maximum box size.\n", size)
		return
	}

	// First check whether the object is in the open box or not. If it's already in the open box,
	// then do nothing cause this box will finally be sealed and added to MRU position in hot queue.
	// If it's not in the open box, then check whether the object is in the sealed boxes with corresponding
	// size range.
	openBox, _ := openBoxes[bound]
	_, ok := openBox.objOffsetMap[id]
	DPrintf("%s is found in open box --> %t.\n", id, ok)

	// object is not in the open box
	if !ok {
		var sealed []int64
		sealed, ok = sealedBoxes[bound]
		foundObject := false
		// check whether it's in sealed boxes or not
		if ok {
			for _, sealedBoxId := range sealed {
				DPrintf("Sealed box id is %d.\n", sealedBoxId)
				sealedBoxPos := boxQueueMap[sealedBoxId]
				element := sealedBoxPos.element.Value.(*Box)
				_, exist := element.objOffsetMap[id]

				// If it's in the sealed box (cached), then update the hot and cold queues
				if exist {
					hits++
					DPrintf("Hits: %d. Object %s is found in sealed box %d ", hits, id, sealedBoxId)
					foundObject = true
					hot := sealedBoxPos.hot
					// In the hot queue, just remove it to MRU position in hot queue. Size is the same
					if hot {
						DPrintf("which was in hot queue.\n")
						hotQueue.Remove(sealedBoxPos.element) // remove it from the hot queue
						hotQueue.PushBack(element)
						newPos := &QueuePos{
							element: 	hotQueue.Back(),
							hot:		true,
						}
						boxQueueMap[element.boxId] = newPos
					} else {
						DPrintf("which was in cold queue.\n")
						coldQueue.Remove(sealedBoxPos.element)	// remove the box from cold queue
						coldSize = coldSize - maxBoxSize

						// In the cold queue, then need to remove it to the MRU position in hot queue.
						updateQueue(element)
					}
					break
				}
			}
		}
		// If the object is not in the sealed boxes, then add it to the corresponding open box
		if !foundObject {
			DPrintf("Object %s is not found.\n", id)
			// If the box cannot hold this object (i.e. full??), seal it and add it to the flash
			// i.e. the MRU position in hot queue. Then create a new open box to hold this object
			if openBox.currSize + objectSize > maxBoxSize {
				// Before add it to hot queue, check whether hot queue if full or not.
				DPrintf("Exceeds! open box %d with upper bound %d currently hold %d bytes and object size is %d.\n",
					openBox.boxId, openBox.upperBound, openBox.currSize, objectSize)

				updateQueue(openBox)

				sealedBoxes[openBox.upperBound] = append(sealedBoxes[openBox.upperBound], openBox.boxId)	// sealed
				frag += (maxBoxSize - openBox.currSize)
				numSeal++
				DPrintf("Box %d has been sealed. There are %d sealed boxes with upper bound %d. Sealed boxes: %d." +
					" Fragmentation: %d.\n",
					openBox.boxId, len(sealedBoxes[openBox.upperBound]), openBox.upperBound, numSeal, frag)

				openBox = &Box{
					boxId: 			nextBoxId,
					currSize:		0,
					objOffsetMap: 	make(map[string]int64),
					upperBound:		bound,
				}
				DPrintf("new open box %d with upper bound %d is created.\n", openBox.boxId, openBox.upperBound)
				nextBoxId++
				openBoxes[bound] = openBox
			}

			openBox.objOffsetMap[id] = openBox.currSize
			openBox.currSize = openBox.currSize + objectSize
			DPrintf("Open box %d with upper bound %d holds %d objects, and current offset is %d.\n",
				openBox.boxId, openBox.upperBound, len(openBox.objOffsetMap), openBox.currSize)
		}
	} else {
		// requested object is in open box.
		hits++
		DPrintf("Hits: %d.\n", hits)
	}

}

/**
	This method is used to update hot and cold queues. "element" is the box to add in the MRU position in hot queue.
	First check whether hot queue if full or not. If hot queue is full, remove the box in the LRU position in hot queue
	to the MRU position in cold queue, where we need to check whether cold queue is full or not. If cold queue is full, evict
	the box in LRU position in cold queue.
	It is safer to check whether the queue is full or not first.
 */
func updateQueue(element *Box) {
	DPrintf("Before updating the queue:")
	PrintQueue(hotQueue, true)
	PrintQueue(coldQueue, false)
	if hotSize + maxBoxSize > maxCacheSize {
		DPrintf("Hot queue is full.\n")
		toBeEvicted := hotQueue.Front()		// the box in LRU position in hot queue
		hotQueue.Remove(hotQueue.Front())
		//updateSealedBoxes(toBeEvicted.Value.(*Box))	// bug: need to remove this box from sealed boxes map
		hotSize = hotSize - maxBoxSize

		// check whether cold queue if full or not. Update cold queue
		if coldSize + maxBoxSize > maxCacheSize {
			DPrintf("Cold queue is full.\n")
			// bug: need to remove this box from sealed boxes map. But since the box evicted from hot queue will be
			// added to the MRU position in cold queue, there is no need to remove it from the mapping. Only need to
			// remove the one which is evicted from the LRU position in cold queue.
			updateSealedBoxes(coldQueue.Front().Value.(*Box))
			coldQueue.Remove(coldQueue.Front())		// evict the box in LRU position
			coldSize = coldSize - maxBoxSize
		}
		coldQueue.PushBack(toBeEvicted.Value.(*Box))
		coldSize = coldSize + maxBoxSize
		newPos := &QueuePos{
			element: 		coldQueue.Back(),
			hot:			false,
		}
		boxQueueMap[coldQueue.Back().Value.(*Box).boxId] = newPos
	}
	hotQueue.PushBack(element)
	hotSize = hotSize + maxBoxSize
	newPos := &QueuePos{
		element: 		hotQueue.Back(),
		hot:			true,
	}
	boxQueueMap[element.boxId] = newPos

	DPrintf("After updating the queue:")
	PrintQueue(hotQueue, true)
	PrintQueue(coldQueue, false)
}

/**
	This function is used to update the mapping from upper bound to sealed boxes id when one box is evicted
	from the hot queue or cold queue.
 */
func updateSealedBoxes(box *Box) {
	DPrintf("Before updating sealed boxes: ")
	boxid := box.boxId		// bug: need to remove this box from sealed boxes map
	upper := box.upperBound
	boxes, ok := sealedBoxes[upper]
	DPrintf("%d.\n", boxes)
	if ok {
		for index, _ := range boxes {
			if boxes[index] == boxid {
				boxes = append(boxes[:index], boxes[index + 1:]...)
				break
			}
		}
	}
	sealedBoxes[upper] = boxes
	DPrintf("After updating sealed boxes: ")
	DPrintf("%d.\n", boxes)
}

/**
	Given the size of new object, get the corresponding upper bound.
 */
func getBound(size int64) int64 {
	var result int64
	result = -1
	for _, bound := range granularity {
		if bound >= size {
			result = bound
			break
		}
	}
	return result
}

/**
	Return experiment results
	1. WCR: waste cache ratio, percentage of wasted space --> bytes of fragmentation / total used cache size
	2. SBRR: sealed box request ratio, number of sealed boxes / number of requests
	3. HRR: hit request ratio, number of hits / number of requests
 */
func Results() (float64, float64, float64) {
	DPrintf("frag: %d, numSeal: %d, numRequest: %d, hits: %d.\n", frag, numSeal, numRequest, hits)
	fmt.Printf("frag: %d, numSeal: %d, numRequest: %d, hits: %d.\n", frag, numSeal, numRequest, hits)
	WCR := float64(frag) / float64(numSeal * maxBoxSize)
	SBRR := float64(numSeal) / float64(numRequest)
	HRR := float64(hits) / float64(numRequest)
	return WCR, SBRR, HRR
}