package ObjectBased

import (
	"math"
	"strings"
	"log"
	"math/rand"
)

var (
	// update every quantum
	E			int64 		// written bytes in this quantum --> real-time
	balance		int64		// balance in all past quantum --> accumulative
)

func ProbSetUp(k int, budget int64) {
	K = k;					// slack variable
	balance = budget;		// budget
	quota = budget;			// initial budget
	E = 0;					// used writes
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

