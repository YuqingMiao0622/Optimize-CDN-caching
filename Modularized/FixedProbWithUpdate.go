package ObjectBased
/**
	Fixed probability is used for each interval. When one interval finishes, compare the erasure bytes
	with budget. If there is remained budget,
 */

var(
	quotaFixed 			int64
	erasureFixed		int64
	higherProb			float64
	lowerProb			float64
	interval			int
	budget				int64
)

/**

 */
func fixedProbTurtleSetUp(quota int64) {
	quotaFixed = quota
	interval = 1
	budget = int64(interval) * quotaFixed
}

