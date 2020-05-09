package awsgetfile

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/magiconair/properties"
)

//GetProperties ...
//To fetc file from aws and return map
func GetProperties(region string, bucket string, fileName string) (properties.Properties, error) {
	data, err := GetAWSFileAsString(region, bucket, fileName)
	return *properties.MustLoadString(data), err
}

func GetAWSFile(region string, bucket string, fileName string) (fp *os.File, err error) {

	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	downloader := s3manager.NewDownloader(sess)
	file, err := os.Create(fileName)
	numBytes, err := downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(fileName),
		})

	if err != nil {
		log.Fatalf("Error while downloading file %q, %v", fileName, err)
	}

	fmt.Println("Got ", file.Name(), numBytes, "bytes")
	return file, err
}

func GetAWSFileAsString(region string, bucket string, fileName string) (string, error) {
	file, errf := GetAWSFile(region, bucket, fileName)
	if errf != nil {
		fmt.Errorf("Error while reading the file:", errf.Error())
		return "", errf
	}
	buf := new(bytes.Buffer)
	_, errb := buf.ReadFrom(file)
	contents := buf.String()
	if errb != nil {
		fmt.Errorf("Error while reading the file:", errb.Error())
	}

	// fmt.Print(contents)
	return contents, errb
}
