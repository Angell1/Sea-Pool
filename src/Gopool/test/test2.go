package main

import (
	"../gopool"
	"fmt"
)

func main() {
	Pool, err := gopool.NewPool(20, 5)
	i := 0
	for i < 50 {
		err = Pool.Submit(Print_Test1, "并发测试！")
		if err != nil {
			fmt.Println(err)
		}
		i++
	}
}

func Print_Test1(str string) error {
	fmt.Println(str)
	return nil
}
