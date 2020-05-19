# awsutilsingo
<ui>
<li>GetProperties: Reads AWS and returns properties as map</li>
<li>GetAWSFile: Reads AWS and returns file</li>
<li>GetAWSFileAsString: Reads AWS and returns file as string</li>
<ui>



# how to use
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
