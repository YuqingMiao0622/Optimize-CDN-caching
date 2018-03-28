package LogStructured

import (
	"strconv"
	"fmt"
)

const Epoch = 1000000
//const Epoch = 1

var (
	MissBytes				int64
	fragRatio 				float64
	SealedBoxRatioTime		[]float64		// how sealed box ratio varies with time
	SealedBoxNumber			[]int64
	HitRatioTime			[]float64		// how hit ratio varies with time
	HitBytesRatioTime		[]float64
	MissBytesRatioTime		[]float64
)

func Request(id string, size string) {
	numRequest++
	getResultsWithTime()
	DPrintf("New request with object id: %s and size: %s. Total requests: %d\n", id, size, numRequest)
	object, err := strconv.Atoi(size)
	objectSize := int64(object)
	if err != nil {
		DPrintf("Input size %s cannot be converted to int64 type with error %s.\n", size, err)
	}
	reqBytes += objectSize

	bound := getBound(objectSize)
	DPrintf("%s should be put into open box with upper bound %d.\n", id, bound)
	if bound == -1 {
		DPrintf("Object size %s exceeds the maximum box size.\n", size)
		return
	}

	// First check whether the object is in open box. If it is, consider as one hit.
	openBox, _ := openBoxes[bound]
	_, ok := openBox.objOffsetMap[id]
	DPrintf("%s is found in open box --> %t.\n", id, ok)

	if !ok {
		// If it is not in open box, then check sealed boxes, use cachedObj map
		boxId, inSealed := cachedObj[id]

		if inSealed {
			// requested object is cached
			hits++
			hitBytes += objectSize
			DPrintf("Hits: %d. Hit bytes: %d. Object %s is found in sealed box %d ",
				hits, hitBytes, id, boxId)

			sealBoxPos := boxQueueMap[boxId]
			element := sealBoxPos.element.Value.(*Box)
			hot := sealBoxPos.hot

			if hot {
				DPrintf("In hot queue.\n")
				// In hot queue, just remove it from original position to the MRU position
				hotQueue.Remove(sealBoxPos.element)
				hotQueue.PushBack(element)
				newPos := &QueuePos{
					element: 	hotQueue.Back(),
					hot:		true,
				}
				boxQueueMap[element.boxId] = newPos
			} else {
				DPrintf("In cold queue.\n")
				coldQueue.Remove(sealBoxPos.element)
				coldSize = coldSize - maxBoxSize
				//coldSize -= objectSize
				updateQueue2(element)
			}
		} else {
			DPrintf("Object %s is not found.\n", id)
			MissBytes += objectSize
			// If the box cannot hold this object (i.e. full??), seal it and add it to the flash
			// i.e. the MRU position in hot queue. Then create a new open box to hold this object
			if openBox.currSize + objectSize > maxBoxSize {
				// Before add it to hot queue, check whether hot queue if full or not.
				DPrintf("Exceeds! open box %d with upper bound %d currently hold %d bytes and object size is %d.\n",
					openBox.boxId, openBox.upperBound, openBox.currSize, objectSize)

				updateQueue2(openBox)
				addObjects(openBox)
				//SealedBoxes[openBox.upperBound] = append(SealedBoxes[openBox.upperBound], openBox.boxId)	// sealed
				frag += (maxBoxSize - openBox.currSize)
				currFrag := float64(maxBoxSize - openBox.currSize) / float64(maxBoxSize)
				fragRatio += currFrag
				numSeal++
				//DPrintf("Box %d has been sealed. There are %d sealed boxes with upper bound %d. Sealed boxes: %d." +
				//	" Fragmentation: %d.\n",
				//	openBox.boxId, len(SealedBoxes[openBox.upperBound]), openBox.upperBound, numSeal, frag)

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
		hits++
		hitBytes += objectSize
		DPrintf("Hits: %d, hit bytes: %d.\n", hits, hitBytes)
	}
	//DDPrintf("Current cached object: %d.\n", len(cachedObj))
}


func updateQueue2(element *Box) {
	DPrintf("Before updating the queue:")
	PrintQueue(hotQueue, true)
	PrintQueue(coldQueue, false)
	if hotSize + maxBoxSize > maxCacheSize {
		DPrintf("Hot queue is full. Hot size: %d, length of hot queue: %d.\n", hotSize, hotQueue.Len())
		//DDPrintf("Hot size: %d, length of hot queue: %d.\n", hotSize, hotQueue.Len())
		toBeEvicted := hotQueue.Front()		// the box in LRU position in hot queue
		hotQueue.Remove(hotQueue.Front())
		//updateSealedBoxes(toBeEvicted.Value.(*Box))	// bug: need to remove this box from sealed boxes map
		hotSize = hotSize - maxBoxSize

		// check whether cold queue if full or not. Update cold queue
		if coldSize + maxBoxSize > maxCacheSize {
			DPrintf("Cold queue is full.\n")
			// the box in LRU position in cold queue is evicted, then objects in that box need to
			// remove from cachedObj map
			//fmt.Println(coldQueue.Len())
			DDPrintf("updateQueue2:: cold queue is full. Cold size: %d. Length of cold queue: %d.\n", coldSize, coldQueue.Len())
			removeObjects(coldQueue.Front().Value.(*Box))
			//updateSealedBoxes(coldQueue.Front().Value.(*Box))
			coldQueue.Remove(coldQueue.Front())		// evict the box in LRU position
			coldSize = coldSize - maxBoxSize
		}
		coldQueue.PushBack(toBeEvicted.Value.(*Box))
		DDPrintf("updateQueue2:: cold queue is not full. Size of cold queue: %d.\n", coldQueue.Len())
		coldSize = coldSize + maxBoxSize
		newPos := &QueuePos{
			element: 		coldQueue.Back(),
			hot:			false,
		}
		boxQueueMap[coldQueue.Back().Value.(*Box).boxId] = newPos
	}
	hotQueue.PushBack(element)
	hotSize = hotSize + maxBoxSize
	DDPrintf("updateQueue2:: Hot size: %d, length of hot queue: %d.\n", hotSize, hotQueue.Len())
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
	When a box is evicted from cold queue, the objects in that box
	need to remove from the Map -- 'cachedObj'
*/
func removeObjects(box *Box) {
	objOffset := box.objOffsetMap
	boxid := box.boxId
	DPrintf("Box %d is evicted which holds %d objects.\n", boxid, len(objOffset))

	for key, _ := range objOffset {
		DPrintf("key is %s.\n", key)
		delete(cachedObj, key)
	}
	DDPrintf("removeObjects:: current cached objects: %d.\n", len(cachedObj))
}

/**
	When a box is sealed, add the objects it holds into cachedObj map
 */
func addObjects(box *Box) {
	objOffSet := box.objOffsetMap
	boxid := box.boxId
	DPrintf("Box %d is sealed and %d objects are added into the cachedObj map.", boxid, len(objOffSet))

	for key, _ := range objOffSet {
		DPrintf("key is %s.\n", key)
		cachedObj[key] = boxid
	}
	DDPrintf("addObjects:: current cached objects: %d.\n", len(cachedObj))
}

/**
	Get results for different metrics every 1 million commands.
	Output:	SealedBoxRatio: sealed box ratio (#sealed box / #requests) varies with time
			SealedBoxNumber: number of sealed boxes
			HitRatiotime: object hit ratio (#read hit / #requests)
			HitBytesRatioTime: bytes hit ratio (# hit bytes / #requests)
			MissBytesRatioTime: optional
 */
func getResultsWithTime() {
	if numRequest % Epoch == 0 {
		DPrintf("ResultsWithTime:: current number of requests is %d.\n", numRequest)
		SealedBoxRatioTime = append(SealedBoxRatioTime, float64(numSeal) / float64(numRequest))
		SealedBoxNumber = append(SealedBoxNumber, numSeal)
		HitRatioTime = append(HitRatioTime, float64(hits) / float64(numRequest))
		HitBytesRatioTime = append(HitBytesRatioTime, float64(hitBytes) / float64(reqBytes))
		MissBytesRatioTime = append(MissBytesRatioTime, float64(MissBytes) / float64(reqBytes))
	}
}

/**
	Return experiment results
	1. WCR: waste cache ratio, percentage of wasted space --> bytes of fragmentation / total used cache size
	2. SBRR: sealed box request ratio, number of sealed boxes / number of requests
	3. HRR: hit request ratio, number of hits / number of requests
 */
func GetResults() (float64, float64, float64, float64) {
	DPrintf("frag: %d, numSeal: %d, numRequest: %d, hits: %d, hit bytes: %d, totoal bytes: %d.\n",
		frag, numSeal, numRequest, hits, hitBytes, reqBytes)
	fmt.Printf("frag: %d, fragRation: %f, numSeal: %d, numRequest: %d, hits: %d, hitBytes: %d, reqBytes: %d.\n",
		frag, fragRatio, numSeal, numRequest, hits, hitBytes, reqBytes)
	WCR := fragRatio / float64(numSeal)
	SBRR := float64(numSeal) / float64(numRequest)
	HRR := float64(hits) / float64(numRequest)
	HBRR := float64(hitBytes) / float64(reqBytes)
	return WCR, SBRR, HRR, HBRR
}

