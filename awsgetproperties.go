package awsgetproperties

import (
	"fmt"

	"github.com/magiconair/properties"
)

func getProperties(bucket string, fileName string) properties.Properties {
	p := properties.MustLoadFile("config.properties", properties.UTF8)
	fmt.Println(p.MustGetString("test"))
	fmt.Println(p.MustGetString("result"))
	return *p
}
