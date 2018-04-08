package ObjectBased

import (
	"container/list"
	"strconv"
	"fmt"
	"math"
	"strings"
	"math/rand"
	"log"
)

const maxBoxSize = 104857600		// 100 MB
const Epoch = 1000000				// 1 million

type Object struct {
	objectId 	string
	objectSize	int64
}

type Box struct {
	boxId		int64					//
	currSize	int64					// record the current size of the box
	upperBound	int64					// the upper bound of object size this box can hold
	objOffsetMap map[string]int64		// map from object id to the offset where this object is stored. --> not required in simulation
}

type QueuePos struct {
	element *list.Element // position in the queue
	hot     bool          // in hot queue or not
}

type AccessCount struct {
	objectId 		string
}

type GhostCache struct {
	maxSize			int64
	currSize		int64
	accessCount		map[string]int	// map. object id --> access count
	queue 			*list.List		// when ghost cache is full, evict some objects.
	objQueueMap		map[string]*list.Element
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
	maxObjSize		int64
	cachedObj		map[string]int64 	// object id --> box id

	/* experiment part */
	numSeal			int64				// number of sealed boxes
	numRequest		int64 				// number of request
	hits			int64				// number of hits
	hitBytes		int64
	reqBytes		int64

	/* over time */
	//MissBytes				int64
	fragRatio 				float64
	SealedBoxRatioTime		[]float64		// how sealed box ratio varies with time
	SealedBoxNumber			[]int64
	HitRatioTime			[]float64		// how hit ratio varies with time
	HitBytesRatioTime		[]float64
	MissBytesRatioTime		[]float64


	/* dynamic granularity */
	count					map[float64]int		// map from power --> number of objects

	/* TIRE */
	ghostCache   *GhostCache
	quota        int64			// quota for each quantum
	quantum      int			// 5 min --> 1 million requests
	K            int			// slack variable
	intervals    []int
	threshold    int
	currInterval int

	// update every quantum
	E			int64 		// written bytes in this quantum --> real-time
	balance		int64		// balance in all past quantum --> accumulative

)

/**
	Set up flash cache.
	Set the maximum cache size and create "number" boxes and "granularity" is used
	to specify the size range (in Bytes) of objects each box is supposed to hold.
	For example, if number = 4, and granularity is {1024, 2048, 4096,100000}.
	Then four boxes are created, and they are supposed to hold objects 0 ~ 1024 Bytes,
	1024 ~ 2048 Bytes, 2048 ~ 4096 Bytes and 4096 ~ 100000 Bytes separately.
 */
func StartUp(cacheSize int64, number int, objSize int64) {
//func StartUp(cacheSize int64, number int, log bool, objSize int64, statPath string) {
	fmt.Println("Modularized test.")
	maxCacheSize = cacheSize / 2
	maxObjSize = objSize
	DDPrintf("StartUp:: Cache size is: %d, cold/hot queue size: %d.\n", cacheSize, maxCacheSize)
	hotQueue = list.New()
	coldQueue = list.New()
	hotSize = 0
	coldSize = 0
	nextBoxId = 1
	openBoxes = make(map[int64]*Box, number)
	granularity = EqualLogGranularity(uint(number))
	fmt.Println(granularity)
	boxQueueMap = make(map[int64]*QueuePos)
	for _, upperBound := range granularity {
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
	basicSetUp()

	// new graph
	timeSetUp()
}

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

func ProbSetUp(k int, budget int64) {
	K = k;					// slack variable
	balance = budget;		// budget
	quota = budget;			// initial budget
	E = 0;					// used writes
}

func basicSetUp() {
	numSeal = 0
	numRequest = 0
	hits = 0

	hitBytes = 0
	reqBytes = 0
	cachedObj = make(map[string]int64)
	count = make(map[float64]int)
}

func timeSetUp() {
	HitRatioTime = make([]float64, 0)
	SealedBoxRatioTime = make([]float64, 0)
	SealedBoxNumber = make([]int64, 0)
	HitBytesRatioTime = make([]float64, 0)
	MissBytesRatioTime = make([]float64, 0)
	//MissBytes = 0
	fragRatio = 0
}

/**
	Deal with new command.
 */
func Request(id string, size string, model string) {
	//fmt.Printf("New request: %s with size %s.\n", id, size)
	DPrintf("Request:: request object %s with size %s.\n", id, size)
	numRequest++
	collectStat(size)		// dynamic granularity

	//if strings.Compare(model, "TIRE") == 0 {
	//	updateTire()
	//} else {
	//	//updateProb()
	//	updateImprovedProb()
	//}

	getResultsWithTime()
	// convert size into integer
	object, err := strconv.Atoi(size)
	objectSize := int64(object)
	if err != nil {
		DPrintf("Input size %s cannot be converted to int64 type with error %s.\n", size, err)
	}
	reqBytes += objectSize

	// get the upper bound --> might be greater than maximum object size --> not allowed
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
		// Not in open boxes
		boxId, isSealed := cachedObj[id]
		if isSealed {
			// object is found in cache
			cachedObject(objectSize, id, boxId)
			updateGhostQueue(id, true)
		} else {
			// Object is not cached. Add it to corresponding open box.
			//MissBytes += objectSize

			// check whether this object can be cached or not
			var admit bool
			if strings.Compare(model, "TIRE") == 0 {
				admit = admissionControlTIRE(id, objectSize)
			} else {
				//admit = admissionControlProb(model, objectSize)
				admit = warmUpPhase(model, objectSize)
			}

			if !admit {
				return
			}
			updateGhostQueue(id, false)
			addToOpenBox(openBox, objectSize, bound, id)
		}
	} else {
		// in open boxes
		hits++
		hitBytes += objectSize
		updateGhostQueue(id, true)
	}

}

/**
	Add one object into corresponding open box. First check whether open box is full or not.
	If it is, add it the the MRU position in cold queue --> Update cold queue, then create a new
	open box to hold this object. Otherwise, add it into the open box.
 */
func addToOpenBox(box *Box, objectSize int64, bound int64, id string) {
	if box.currSize + objectSize > maxBoxSize {
		// open box is full --> seal.
		updateColdQueue(box)
		addObjects(box)
		fragRatio += float64(maxBoxSize - box.currSize) / float64(maxBoxSize)
		numSeal++

		box = &Box{nextBoxId, 0,  bound, make(map[string]int64)}
		nextBoxId++
		openBoxes[bound] = box
	}
	box.objOffsetMap[id] = box.currSize
	box.currSize += objectSize
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
	Object is cached. If in hot queue, just update the hot queue. Otherwise, update both hot and cold queue.
 */
func cachedObject(objectSize int64, id string, boxId int64) {
	DPrintf("cachedObject:: object %s is cached in box %d.\n", id, boxId)
	hits++
	hitBytes += objectSize

	sealedBoxPos := boxQueueMap[boxId]
	element := sealedBoxPos.element.Value.(*Box)

	if sealedBoxPos.hot {
		// sealed box is in hot queue
		DPrintf("cachedObject:: sealed box is in hot queue.\n")
		removeFromQueue(sealedBoxPos.element, true)
		pushToQueue(element, true)
	} else {
		// sealed box is in cold queue
		DPrintf("cachedObject:: sealed box is in cold queue.\n")
		removeFromQueue(sealedBoxPos.element, false)
		updateHotQueue(element)
	}
}

/**
	Push input box into the queue. Input parameter 'hot' determines which queue this box
	is pushed into --> true: hot, false: cold
	Update size of the corresponding queue.
 */
func pushToQueue(box *Box, hot bool) {
	var newPos *QueuePos
	if hot {
		hotQueue.PushBack(box)
		newPos = &QueuePos{hotQueue.Back(), true}
		hotSize += maxBoxSize
	} else {
		coldQueue.PushBack(box)
		newPos = &QueuePos{coldQueue.Back(), false}
		coldSize += maxBoxSize
	}
	boxQueueMap[box.boxId] = newPos
}

/**
	Remove box from queue and update size of the corresponding queue.
 */
func removeFromQueue(element *list.Element, hot bool) {
	//DPrintf("removeFromQueue:: before removing box %d from queue.\n", element.Value.(*Box).boxId)

	if hot {
		hotQueue.Remove(element)
		hotSize -= maxBoxSize
	} else {
		coldQueue.Remove(element)
		coldSize -= maxBoxSize
	}
}

/**
	Update cold queue. First, check whether cold queue is full. If it is, then evict the box in LRU position
	of cold queue. Otherwise, do nothing. Then add the input 'box' into the MRU position in cold queue.
 */
func updateColdQueue(box *Box) {
	DPrintf("updateColdQueue:: before updating: " )
	PrintQueue(hotQueue, false)
	if coldSize + maxBoxSize > maxCacheSize {
		// cold queue is full
		removeObjects(coldQueue.Front().Value.(*Box))
		removeFromQueue(coldQueue.Front(), false)
	}
	pushToQueue(box, false)
	DPrintf("updateColdQueue:: after updating: " )
	PrintQueue(hotQueue, false)
}

/**
	Update hot queue. First check whether hot queue is full or not. If it is, then evict the box in the LRU position
	in hot queue which will be pushed into the LRU position in cold queue. Otherwise, do nothing.
	Then add the input 'box' into the MRU position in hot queue.
 */
func updateHotQueue(box *Box) {
	DPrintf("updateHotQueue:: before updating: " )
	PrintQueue(hotQueue, true)
	if hotSize + maxBoxSize > maxCacheSize {
		// hot queue is full.
		updateColdQueue(hotQueue.Front().Value.(*Box))
		removeFromQueue(hotQueue.Front(), true)
	}
	pushToQueue(box, true)
	DPrintf("updateHotQueue:: after updating: " )
	PrintQueue(hotQueue, true)
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

/**
	Reset the erasure bytes when one quantum finishes.
 */
func updateProb() {
	if numRequest % Epoch == 0 {
		DFmtPrintf("updateProb:: Requests: %d, erasure bytes during last quantum: %d.\n", numRequest, E)
		E = 0;
	}
}

/**
	Update budget for current interval and reset the used bytes to 0 when one interval finishes.
 */
func updateImprovedProb() {
	if numRequest % Epoch == 0 {
		DFmtPrintf("updateImprovedProb:: Requests: %d, used bytes: %d, last balance: %d.\n", numRequest, E, balance)
		balance += quota - E
		E = 0
	}
}

/**
	Check whether current request is in warm up phase.
	Warm up phase: there is no budget for the first 250 million requests.
	Return true if current request is within the warm up phase. Otherwise, use the improved probability
	admission control to determine whether this missed object is cached or not.
 */
func warmUpPhase(line string, size int64) bool {
	if numRequest <= 250 * Epoch {
		return true
	} else {
		updateImprovedProb()
		return admissionControlImprovedProb(line, size)
	}
}


/**
	Combine probability admission control with TIRE "penalty" across time.
*/
func admissionControlImprovedProb(line string, size int64) bool {
	var prob float64
	if strings.Compare(line, "lameDuck") == 0 {
		prob = improvedLameDuck()
	} else if strings.Compare(line, "angryBird") == 0 {
		prob = improvedAngryBird()
	} else {
		log.Fatalf("Wrong choice of probability. Should be lameDuck or angryBird!")
	}

	random := rand.Float64()
	var admit bool
	admit = random <= prob
	if admit {
		E += size
	}
	//DFmtPrintf("admissioControlImprovedProb:: prob: %f, random: %f, admit: %t.\n", prob, random, admit)
	return admit
}


/**
	Admission control using probability. Three probability distributions can be used.
	Lame duck: line
	Spicy chicken: exponential
	Angry bird: logarithm
 */
func admissionControlProb(line string, size int64) bool {
	var prob float64
	if strings.Compare(line, "lameDuck") == 0 {
		prob = lameDuck()
	} else if strings.Compare(line, "spicyChicken") == 0 {
		prob = spicyChicken()
	} else if strings.Compare(line, "angryBird") == 0 {
		prob = angryBird()
	}

	random := rand.Float64()
	var admit bool
	admit = random <= prob
	if admit {
		E += size
	}
	//DFmtPrintf("admissionControlProb:: requests: %d, prob: %f, random: %f, admit: %t.\n", numRequest, prob, random, admit)
	return admit
}

/**
	Probability: line
 */
func lameDuck() float64 {
	prob := -1 / float64(int64(K) * quota) * float64(E) + 1;
	return prob
}

/**
	Improved probability: the budget varies with intervals --> line
 */
func improvedLameDuck() float64 {
	var prob float64
	if balance <= 0 {
		prob = 0
	} else {
		prob = -1 / float64(int64(K) * balance) * float64(E) + 1;
	}
	return prob
}

/**
	Probability: exponential
 */
func spicyChicken() float64 {
	prob := math.Exp(float64(-E) / float64(quota))
	return prob
}

/**
	Probability: logarithm
 */
func angryBird() float64 {
	prob := math.Log(float64(K + 1) - float64(E) / float64(quota)) / math.Log(5)
	//prob := math.Log(float64(E - int64(K) * quota))
	return prob
}

func improvedAngryBird() float64 {
	var prob float64
	if balance <= 0 {
		prob = 0
	} else {
		prob = math.Log(float64(K + 1) - float64(E) / float64(quota)) / math.Log(5)
	}
	return prob
}

/**
	Return experiment results
	1. WCR: waste cache ratio, percentage of wasted space --> bytes of fragmentation / total used cache size
	2. SBRR: sealed box request ratio, #sealed boxes / #requests
	3. OHR: object hit ratio, #read hit / #requests
	4. BHR: bytes hit ratio, #hit bytes / #requests
 */
func GetResults() (float64, float64, float64, float64) {
	DPrintf("numSeal: %d, numRequest: %d, hits: %d, hit bytes: %d, totoal bytes: %d.\n",
		numSeal, numRequest, hits, hitBytes, reqBytes)
	fmt.Printf("fragRation: %f, numSeal: %d, numRequest: %d, hits: %d, hitBytes: %d, reqBytes: %d.\n",
		fragRatio, numSeal, numRequest, hits, hitBytes, reqBytes)
	WCR := fragRatio / float64(numSeal)
	SBRR := float64(numSeal) / float64(numRequest)
	OHR := float64(hits) / float64(numRequest)
	BHR := float64(hitBytes) / float64(reqBytes)
	return WCR, SBRR, OHR, BHR
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
		//MissBytesRatioTime = append(MissBytesRatioTime, float64(MissBytes) / float64(reqBytes))
	}
}

func GetLength() int {
	return ghostCache.queue.Len()
}
