package testutils

import "fmt"

func ExampleCartesianProduct() {
	intCartesianProduct := func(data [][]int) [][]int {
		var out [][]int
		create := func(n int) {
			out = make([][]int, n)
		}
		set := func(outIdx, dim, dimPos int) {
			if out[outIdx] == nil {
				out[outIdx] = make([]int, len(data))
			}
			out[outIdx][dim] = data[dim][dimPos]
		}
		lengths := make([]int, 0, len(data))
		for _, d := range data {
			lengths = append(lengths, len(d))
		}
		CartesianProduct(create, set, lengths...)
		return out
	}
	inputs := [][]int{
		{1, 2, 3},
		{11, 22},
		{111},
		{1111, 2222},
	}
	fmt.Println("inputs:")
	for _, p := range inputs {
		fmt.Println(p)
	}
	fmt.Println()
	outputs := intCartesianProduct(inputs)
	fmt.Println("outputs:")
	for _, p := range outputs {
		fmt.Println(p)
	}

	// Output:
	// inputs:
	// [1 2 3]
	// [11 22]
	// [111]
	// [1111 2222]
	//
	// outputs:
	// [1 11 111 1111]
	// [1 11 111 2222]
	// [1 22 111 1111]
	// [1 22 111 2222]
	// [2 11 111 1111]
	// [2 11 111 2222]
	// [2 22 111 1111]
	// [2 22 111 2222]
	// [3 11 111 1111]
	// [3 11 111 2222]
	// [3 22 111 1111]
	// [3 22 111 2222]
}
