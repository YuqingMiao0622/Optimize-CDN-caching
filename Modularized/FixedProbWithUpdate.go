package ObjectBased

import (
	"math/rand"
	"strings"
	"log"
)

/**
	Fixed probability is used for each interval. When one interval finishes, compare the erasure bytes
	with budget. If there is remained budget,
 */

var(
	quotaFixed 			int64
	erasureFixed		int64
	higherProb			float64
	//lowerProb			float64
	//interval			int
	//budget				int64
	fixedProb			float64
)

/**

 */
func FixedProbSetUp(quota int64) {
	quotaFixed = quota
	//interval = 1
	//budget = int64(interval) * quotaFixed
	fixedProb = 1
	higherProb = 1
	//lowerProb = 0
	erasureFixed = 0
}

/**
	When one interval finishes, update the fixed probability for next interval based on the erasure bytes.
	If erasure bytes exceed given budget, then reduce the fixed probability to half for the next interval.
	Otherwise, average the last above (higherProb) and last below (fixed)

	1. WhiteBear
	2. SmilingTurtle
 */
func updateFixedProb(method string) {
	if numRequest % Epoch != 0 || numRequest < 250 * Epoch {
		return
	}
	var budget int64
	if strings.Compare(method, "whiteBear") == 0 {
		budget = quotaFixed
	} else if strings.Compare(method, "smilingTurtle") == 0 {
		budget = (numRequest / Epoch - 249) * quotaFixed
	} else {
		log.Fatalf("Wrong method! Should be whiteBear or smilingTurtle.\n")
	}
	DFmtPrintf("updateFixedProb:: number of requests: %d, original probability: %f, erasure bytes: %d and budget: %d. ",
		numRequest, fixedProb, erasureFixed, budget)
	if erasureFixed > budget {
		higherProb = fixedProb
		fixedProb /= 2
	} else {
		fixedProb = (higherProb + fixedProb) / 2
	}
	if strings.Compare(method, "whiteBear") == 0 {
		erasureFixed = 0
	}
	DFmtPrintf("Updated prob: %f, erasure bytes: %d.\n", fixedProb, erasureFixed)
}

func warmUpFixedProb(method string, size int64) bool {
	if numRequest < 250 * Epoch {
		return true
	} else {
		//if numRequest % Epoch == 0 {
		//	DFmtPrintf("warmUpFixedProb:: number of requests: %d.\n", numRequest)
		//	updateFixedProb(method)
		//}
		return admissionControlFixedProb(size)
	}
}

func admissionControlFixedProb(size int64) bool {
	random := rand.Float64()
	if random < fixedProb {
		erasureFixed += size
		return true
	} else {
		return false
	}
}
