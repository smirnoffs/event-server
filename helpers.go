package main


func getKeys(m map[int64]bool) []int64{
	keys := make([]int64, len(m))

	i := 0
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}