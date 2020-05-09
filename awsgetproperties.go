package awsgetproperties

import (
	"fmt"

	"github.com/magiconair/properties"
)

//GetProperties ...
//To fetc file from aws and return map
func GetProperties(bucket string, fileName string) properties.Properties {
	p := properties.MustLoadString("test=test2 \n result=result3")
	fmt.Println(p.MustGetString("test"))
	fmt.Println(p.MustGetString("result"))
	return *p
}
