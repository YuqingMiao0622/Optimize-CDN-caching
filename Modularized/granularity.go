package ObjectBased

import (
	"strconv"
	"os"
	"log"
	"bufio"
	"strings"
	"math"
	"sort"
)


/*
	One definition of granularity --> equal logarithmic
 */
func EqualLogGranularity(number uint) []int64 {
	n := uint64(maxObjSize)
	//n = uint64(64)
	digit := len(strconv.FormatUint(n, 2))
	//fmt.Printf("Digit is : %d.\n", digit)

	var granularity []int64
	base := uint(digit) / number

	for index := uint(0); index < number - 1; index++ {
		shift := base * (index + 1)
		granularity = append(granularity, 1 << shift)
	}
	granularity = append(granularity, maxObjSize)

	return granularity
}

func collectStat(size string) {
	number, _ := strconv.ParseFloat(size, 64)
	power := toFixed(math.Log10(number), 1)
	count[power]++

	if numRequest % Epoch == 0 {
		DynamicGranularity(len(granularity))
	}
}

func round(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

func toFixed(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(round(num * output)) / output
}

/**
	Compute granularity dynamically
 */
func DynamicGranularity(number int) {
	// sort the count map based on the key
	intervals := make([]float64, 0)
	for power := range count {
		intervals = append(intervals, power)
	}
	sort.Float64s(intervals)

	counts := make([]int, 0)
	total := 0
	for _, power := range intervals {
		total += count[power]
		counts = append(counts, total)
	}
	//fmt.Println(counts)
	//fmt.Println(intervals)

	base := int(numRequest) / number
	tempGran := make([]int64, 0)
	for i := 1; i < number; i++ {
		tempGran = append(tempGran, int64(math.Pow(10, getIntervals(base * i, counts, intervals))))
	}
	tempGran = append(tempGran, maxObjSize)
	updateUpperBound(tempGran)

	//fmt.Printf("Granularity is updated. Current granularity is: %v.\n", tempGran)
	granularity = tempGran
}

func updateUpperBound(tempGran []int64) {
	newOpenBoxes := make(map[int64]*Box)
	for index, value := range granularity {
		box := openBoxes[value]
		box.upperBound = tempGran[index]
		newOpenBoxes[tempGran[index]] = box
	}
	openBoxes = newOpenBoxes
	//fmt.Println("Open boxes are updated. ", openBoxes)
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
	//fmt.Println(counts)
	//fmt.Println(intervals)

	base := count / box
	for i := 0; i < box; i++ {
		result = append(result, getIntervals(base * (i + 1), counts, intervals))
	}
	//fmt.Println(result)
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
