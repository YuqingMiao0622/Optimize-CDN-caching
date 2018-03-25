package ObjectBased

import (
	"strconv"
	"fmt"
	"os"
	"log"
	"bufio"
	"strings"
)


/*
	One definition of granularity --> equal logarithmic
 */
func EqualLogGranularity(number uint) []int64 {
	n := uint64(maxObjSize)
	//n = uint64(64)
	digit := len(strconv.FormatUint(n, 2))
	fmt.Printf("Digit is : %d.\n", digit)

	var granularity []int64
	base := uint(digit) / number

	for index := uint(0); index < number - 1; index++ {
		shift := base * (index + 1)
		granularity = append(granularity, 1 << shift)
	}
	granularity = append(granularity, maxObjSize)

	return granularity
}

/**
	Get granularity based on the y axis
 */
func YaxisGranularity(box int, filePath string) []float64 {
	result := make([]float64, 0)
	file, err := os.Open(filePath)
	if (err != nil) {
		log.Fatalf("Cannot open file %s --> %s.\n", filePath, err)
	}
	defer file.Close()

	counts := make([]int, 0)		// total counts
	intervals := make([]float64, 0)	// intervals
	var count int
	scanner := bufio.NewScanner(file)
	for (scanner.Scan()) {
		line := scanner.Text()
		//fmt.Println(line)
		tokens := strings.Fields(line)

		curr, _ := strconv.Atoi(tokens[1])
		count += curr
		counts = append(counts, count)

		inter, _ := strconv.ParseFloat(tokens[0], 32)
		intervals = append(intervals, inter)
	}
	fmt.Println(counts)
	fmt.Println(intervals)

	base := count / box
	for i := 0; i < box; i++ {
		result = append(result, getIntervals(base * (i + 1), counts, intervals))
	}
	fmt.Println(result)
	return result
}

func getIntervals(number int, counts []int, intervals []float64) float64 {
	for index, num := range counts {
		if num >= number {
			return intervals[index]
		}
	}
	return 0;
}
