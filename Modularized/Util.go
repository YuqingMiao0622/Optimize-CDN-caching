package ObjectBased

import (
	"os"
	"log"
	"container/list"
	"fmt"
)

const flag = 2

var (
	log_file, _ = os.OpenFile("logger.txt", os.O_CREATE | os.O_RDWR | os.O_TRUNC, 0)
	logger = log.New(log_file, "Log Structured----", log.Lshortfile | log.Lmicroseconds)
)

func DFmtPrintf(format string, v ...interface{}) {
	if flag == 2 {
		fmt.Printf(format, v...)
	}
}

func DPrintf(format string, v ...interface{}) {
	if flag == 1 {
		//fmt.Printf(format, v...)
		logger.Printf(format, v...)
	}
}

func DDPrintf(format string, v ...interface{}) {
	if flag == -2 {
		logger.Printf(format, v...)
	}
}

func PrintQueue(queue *list.List, hot bool) {
	if flag == 1 {
		if hot {
			logger.Println("Current hot queue: ")
		} else {
			logger.Println("Current cold queue: ")
		}

		for element := queue.Front(); element != nil; element = element.Next() {
			box := element.Value.(*Box)
			logger.Printf("Box id %d holds %d items, current size is %d.\n",
				box.boxId, len(box.objOffsetMap), box.currSize)
		}
		logger.Println()
	}
}

func PrintElement(e *list.Element) {
	box := e.Value.(*Box)
	if flag == 1 {
		logger.Printf("Box id %d with upper bound %d holding %d items.\n",
			box.boxId, box.upperBound, len(box.objOffsetMap))
	}
}
