# awsutilsingo
package main

How to use?

import (
	"fmt"

	awsprop "github.com/sureshatgithub/awsutilsingo"
)

func main() {
	p := awsprop.GetProperties("region", "bucket-name", "fileName")
	fmt.Println(p.MustGetString("property-key"))
}
