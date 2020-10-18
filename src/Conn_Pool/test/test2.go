package main

import (
	"fmt"
	"time"
)

func main() {
	//print(time.Second)
	fmt.Println(time.Now().UnixNano())

	fmt.Println(time.Now().Add(time.Second).UnixNano())
}
