package expr

// courtesy of https://gist.github.com/cevaris/bc331cbe970b03816c6b
func fuzzyEquals(epsilon, a, b float64) bool {
	return (a-b) < epsilon && (b-a) < epsilon
}
