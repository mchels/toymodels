package main

import (
	"fmt"
)

// type foo struct {
	// 	var1 int
	// 	var2 string
// }

type Foo struct {
	x string
	y string
}

func main() {
	foo := Foo{y: "aaa"}
	// var var1 uint64
	// var var2 uint64
	// var1 = 10
	// var2 = 20
	fmt.Printf("%+v", foo)
	fmt.Println(foo.x)
	fmt.Println(foo.y)
	fmt.Println(foo.x == "")
}
