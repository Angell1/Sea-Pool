package main

func main() {
	var a = [...]int{1, 2, 3}
	l := len(a) - 1
	println(l)
	println(len(a))
	for i, v := range a {
		println(i, v)
	}
	println()
	for i, v := range a[0:] {
		println(i, v)
	}

}
