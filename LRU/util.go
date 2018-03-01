package base

import (
	"fmt"
	"container/list"
	"log"
	"os"
	"awesomeProject/LogStructured"
)

const flag = 1

var (
	log_file, _ = os.OpenFile("log.txt", os.O_CREATE | os.O_RDWR, 0)
	logger = log.New(log_file, "Log Structured----", log.Lshortfile | log.Lmicroseconds)
)

func printQueue(queue *list.List) {
	if flag > 0 {
		for element := queue.Front(); element != nil; element = element.Next() {
			fmt.Print(element.Value.(*Object).objectID)
			fmt.Print(" ")
		}
		fmt.Println()
	}
	return
}

func printElement(e *list.Element) {
	if flag > 0 {
		fmt.Printf("Current object ID is: %s.\n", e.Value.(*Object).objectID)
	}
	return
}

func PrintQueue(queue *list.List) {
	if flag > 0 {
		for element := queue.Front(); element != nil; element = element.Next() {
			logger.Printf("Box id %s holds %d items, current size is %d.\n",
				element.Value.(*LogStructured.Box))
		}
		logger.Println()
	}
}

func DPrintf(format string, v ...interface{}) {
	if flag > 0 {
		//fmt.Printf(format, v...)
		logger.Printf(format, v...)
	}
}