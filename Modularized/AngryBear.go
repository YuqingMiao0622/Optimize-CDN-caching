package ObjectBased

import (
	"math"
	"math/rand"
	"fmt"
)

const Grain = 10000

var (
	avgProb 		float64
	admitMiss		int64
	totalMiss		int64
	written			int64
	budget 			int64
	quotaABear		int64
)

/**
	Set up.
 */
func AngryBearSetUp(quota int64) {
	quotaABear = quota
	written = 0
	admitMiss = 0
	totalMiss = 0
	budget = quota
	avgProb = 1
}

/**
	Get the admission probability with a given written bytes
 */
func angryBearProb() float64 {
	var prob float64
	prob = math.Log(float64(budget - written)) / math.Log(float64(budget))
	return prob
}

/**
	Update the average admission probability and budget. The first 250 million requests are warm up phase.
 */
func updateAvgProb() {
	// Interval is not finished or warm up phase
	if numRequest % Epoch != 0 || numRequest < 250 * Epoch {
		return
	}
	DFmtPrintf("updateAvgProb:: number of requests: %d. Original avgProb: %f, written: %d, budget: %d. Total miss: %d, admitted: %d.\n",
		numRequest, avgProb, written, budget, totalMiss, admitMiss)
	if numRequest == 250 * Epoch {
		avgProb = 1
	} else {
		avgProb = float64(admitMiss) / (float64(totalMiss) * avgProb)
	}

	budget = quotaABear + budget - written
	DFmtPrintf("updateAvgProb:: current avgProb is %f, current budget is %d.\n", avgProb, budget)
	written = 0
	admitMiss = 0
	totalMiss = 0
}

/**
	Based on the probability obtained from the logarithmic distribution and the actual written bytes,
	determine whether this object can be cached or not.
 */
func admissionControlAngryBear(size int64) bool {
	prob := angryBearProb()

	if prob > 0 {
		random := rand.Float64()
		if random < prob {
			written += size
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func warmUpAngryBear(size int64) bool {
	if numRequest / Epoch < 250 {
		return true
	}
	return admissionControlAngryBear(size)
}

/**
	Get results for different metrics every 1 million commands.
	Output:	SealedBoxRatio: sealed box ratio (#sealed box / #requests) varies with time
			SealedBoxNumber: number of sealed boxes
			HitRatiotime: object hit ratio (#read hit / #requests)
			HitBytesRatioTime: bytes hit ratio (# hit bytes / #requests)
			MissBytesRatioTime: optional
 */
func getResultsWithTimeFineGrain() {
	if numRequest % Grain == 0 {
		//DFmtPrintf("getResultsWithTimeFineGrain:: current number of requests: %d.\n", numRequest)
		NumberOfRequests = append(NumberOfRequests, numRequest)
		SealedBoxRatioTime = append(SealedBoxRatioTime, float64(numSeal) / float64(numRequest))
		SealedBoxNumber = append(SealedBoxNumber, numSeal)
		HitRatioTime = append(HitRatioTime, float64(hits) / float64(numRequest))
		HitBytesRatioTime = append(HitBytesRatioTime, float64(hitBytes) / float64(reqBytes))
		//MissBytesRatioTime = append(MissBytesRatioTime, float64(MissBytes) / float64(reqBytes))
	}
}

/**
	Return experiment results
	1. WCR: waste cache ratio, percentage of wasted space --> bytes of fragmentation / total used cache size
	2. SBRR: sealed box request ratio, #sealed boxes / #requests
	3. OHR: object hit ratio, #read hit / #requests
	4. BHR: bytes hit ratio, #hit bytes / #requests
 */
func GetResultsFineGrain() (float64, float64, float64, float64) {
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