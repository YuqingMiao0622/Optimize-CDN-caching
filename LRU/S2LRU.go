package LRU

import (
	"container/list"
	"fmt"
	"strconv"
)

var (
	maxCacheSize int
	coldQueue *list.List	// FIFO --> doubly linked list --> LRU in the front, MRU in the end
	hotQueue *list.List
	objQueueMap	map[string]*QueuePos	// object id is the key and corresponding position (i.e. index) in cold queue is the value.
	//sizeMap map[string]int  		// object id --> object size
	hotSize int
	coldSize int
)

type Object struct {
	objectID 	string
	objectSize	int
}

type QueuePos struct {
	pos 	*list.Element
	hot		bool		// whether is in hot queue or not
}

func LruCache(size int) {
	maxCacheSize = size / 2
	coldQueue = list.New()
	hotQueue = list.New()
	objQueueMap = make(map[string]*QueuePos, 0)
	hotSize = 0
	coldSize = 0
	//sizeMap = make(map[string]int, 0)
}

func Request(object string, size string) {
	fmt.Printf("Requested object: %s.\n", object)

	// Question: need to update sizeMap or not when object is in the cache but the request is asking for a different size
	objectSize, err := strconv.Atoi(size)
	if err != nil {
		fmt.Printf("Cannot convert size %s to integer.\n", objectSize)
	}

	element, ok := objQueueMap[object]
	if ok {
		// object is in cache, before updating the LRU queue, we need to make sure that this object is up-to-date.
		// i.e, check the size of the object.
		origSize := element.pos.Value.(*Object).objectSize

		// create a new instance.
		obj := &Object{
			objectID: 		object,
			objectSize: 	objectSize,
		}

		// If it is out of date, then we think it is a miss, put it into the MRU position in cold queue
		if objectSize != origSize {
			if element.hot {
				hotQueue.Remove(element.pos)
			} else {
				coldQueue.Remove(element.pos)
			}
			delete(objQueueMap, object)
			coldQueue.PushBack(obj)

			newPos := &QueuePos{
				pos:	coldQueue.Back(),
				hot:	false,
			}
			objQueueMap[object] = newPos

			coldSize = coldSize + objectSize
			if coldSize > maxCacheSize {
				updateColdQueue()
			}
		} else {
			// Object is up-to-date
			if element.hot {
				// object is in hot queue， moving it to MRU position doesn't change the hot cache size
				hotQueue.Remove(element.pos)	// remove it from the queue
				hotQueue.PushBack(obj)			// insert it to MRU position in the hot queue

				newPos := &QueuePos{
					hot:	true,
					pos:	hotQueue.Back(),
				}
				objQueueMap[obj.objectID] = newPos		// update object id --> position in the queue
			} else {
				// check whether hot queue is full or not
				// hot queue is full, remove the LRU objects into the MRU position in cold queue, then insert
				// the requested object into the MRU position in hot queue
				coldQueue.Remove(element.pos)	// remove it from cold queue
				coldSize = coldSize - objectSize		// update size of cold queue
				hotQueue.PushBack(obj)		// add it to hot queue

				newPos := &QueuePos{
					hot:	true,
					pos:	hotQueue.Back(),
				}
				objQueueMap[object] = newPos		// update object id --> position

				hotSize = hotSize + objectSize		//update size of hot queue
				if hotSize > maxCacheSize {

					toBeTrans := updateHotQueue()

					// add the objects evicted from LRU positions in hot queue to MRU positions in cold queue
					for _, evicted := range toBeTrans {
						transObj := &Object {
							objectID:		evicted.objectID,
							objectSize:		evicted.objectSize,
						}
						coldQueue.PushBack(transObj)		// add it to MRU position in cold queue
						coldSize = coldSize + transObj.objectSize		// update size
						transPos := &QueuePos{
							hot:	false,
							pos:	coldQueue.Back(),
						}
						objQueueMap[transObj.objectID] = transPos		// update position information
					}

					if coldSize >maxCacheSize {
						updateColdQueue()
					}
				}
			}
		}
	} else {
		// if it is new data, then insert it to the MRU position in cold queue.
		// Similarly, need to check whether cold queue is full or not.
		// cache is full, remove the least recently used object from LRU queue and map
		newObject := &Object {
			objectID:		object,
			objectSize: 	objectSize,
		}
		coldQueue.PushBack(newObject)

		newPos := &QueuePos{
			hot:	false,
			pos:	coldQueue.Back(),
		}
		objQueueMap[object] = newPos

		coldSize = coldSize + objectSize
		if coldSize > maxCacheSize {
			updateColdQueue()
		}
	}
}

func updateHotQueue() []*Object {
	toBeTrans := make([]*Object, 0)
	for e := hotQueue.Front(); e != nil && hotSize > maxCacheSize; e = e.Next() {
		objectid := e.Value.(*Object).objectID
		objSize := e.Value.(*Object).objectSize
		hotQueue.Remove(e)		// remove from hot queue
		delete(objQueueMap, objectid)	// remove it from object id --> position map
		hotSize = hotSize - objSize
		evicted := &Object{
			objectSize:		objSize,
			objectID: 		objectid,
		}
		toBeTrans = append(toBeTrans, evicted)		// add it to slice which will be added to cold queue
	}
	return toBeTrans
}

//func updateHotQueue() []string{
//	toBeTrans := make([]string, 0)
//	for e := hotQueue.Front(); e != nil && hotSize > maxCacheSize; e = e.Next() {
//		objectid := e.Value.(*Object).objectID
//		hotQueue.Remove(e)		// remove from hot queue
//		delete(objQueueMap, objectid)	// remove it from object id --> position map
//		hotSize = hotSize - sizeMap[objectid]
//		toBeTrans = append(toBeTrans, objectid)		// add it to slice which will be added to cold queue
//	}
//	return toBeTrans
//}

func updateColdQueue() {
	for e := coldQueue.Front(); e != nil && coldSize > maxCacheSize; e = e.Next() {
		objectid := e.Value.(*Object).objectID
		objectsize := e.Value.(*Object).objectSize
		coldQueue.Remove(e)
		delete(objQueueMap, objectid)
		coldSize = coldSize - objectsize
	}
}

