package base

import (
	"fmt"
	"bufio"
	"os"
	"strings"
	"strconv"
)

func ReadTrace(filepath string, trace map[string]string)  {
	file, err := os.Open(filepath);
	if err != nil {
		fmt.Printf("Cannot open file %s with error %s.\n", filepath, err);
	}

	defer file.Close()	// check

	scanner := bufio.NewScanner(file);
	count := 0
	total_num := 0
	total_size := 0
	for scanner.Scan() {
		line := scanner.Text();
		//fmt.Println(line)
		tokens := strings.Fields(line)  		// split strings by one or more consecutive white spaces.
		//fmt.Println(tokens)
		total_num++
		num, _ := strconv.Atoi(tokens[2])
		total_size += num
		//if num > 16777216 {
		//	count++
		//}
		_, ok := trace[tokens[1]]
		if ok {
			count++
		}
		//element, ok := trace[tokens[1]];
		//if ok && element != tokens[2] {
		//	fmt.Printf("Object %s already exists with size %d, while current size is %s.\n", tokens[1], element, tokens[2])
		//}
		trace[tokens[1]] = tokens[2]
	}
	fmt.Printf("Count is %d. Total number is %d. Total size is %d\n", count, total_num, total_size)
}