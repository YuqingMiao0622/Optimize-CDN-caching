package LRU

import "container/list"
import "fmt"
import "strconv"

var maxSize int
var Queue *list.List                    // FIFO --> doubly linked list --> LRU in the front, MRU in the end
var coldMap	map[string]*list.Element // object id is the key and corresponding position (i.e. index) in cold queue is the value.
var currSize int
var objSizeMap map[string]int  		// object id --> object size

func lruCache(size int) {
	maxSize = size
	Queue = list.New()
	coldMap = make(map[string]*list.Element, 0)
	currSize = 0;
	objSizeMap = make(map[string]int, 0)
}

func request(object string, size string) {
	fmt.Printf("Requested object: %s.\n", object)
	element, ok := coldMap[object]
	if ok {
		// object is in cache, update the LRU queue
		// Question: need to update sizeMap or not????
		obj := &Object{
			objectID: element.Value.(*Object).objectID,
		}
		fmt.Printf("Object exists. Current queue is: ")
		printQueue(Queue)
		Queue.Remove(element)
		printQueue(Queue)
		Queue.PushBack(obj)
		printQueue(Queue)
		coldMap[object] = Queue.Back()

	} else {
		objectSize, err := strconv.Atoi(size)
		if err != nil {
			fmt.Printf("Cannot convert size %s to integer.\n", objectSize)
		} else {
			objSizeMap[object] = objectSize
		}
		// cache is full, remove the least recently used object from LRU queue and map
		if currSize + objectSize > maxSize {
			for element = Queue.Front(); element != nil; element = element.Next() {
				obj := element.Value.(*Object).objectID
				Queue.Remove(element)
				delete(coldMap, obj)
				//delete(sizeMap, object)
				if maxSize - objSizeMap[obj] > objectSize {
					break;
				}
			}
		}
		newObject := &Object {
			objectID:	object,
		}
		Queue.PushBack(newObject)
		coldMap[object] = Queue.Back()
		currSize = currSize + objectSize
		fmt.Printf("New object %s is inserted. Current map is: ", object)
		printQueue(Queue)
	}
}
