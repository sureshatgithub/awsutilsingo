package awsgetfile

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/magiconair/properties"
)

//GetProperties ...
//To fetch file from aws and return map
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

//DownloadDir ...
func DownloadDir(region string, bucket string, destLocalDir string, sourceAWSDir string, clearDest bool) (string, error) {
	if clearDest {
		cleardestLocalDirListing(destLocalDir)
	}
	getS3Objects(region, bucket, destLocalDir, sourceAWSDir)
	fmt.Printf("Total %d files downloaded from s3 bucket\n",
		downloadedFileCount)
	return "Files downloaded", nil
}
func cleardestLocalDirListing(destLocalDir string) {
	os.RemoveAll(destLocalDir + "data/*")
}
func getS3Objects(region string, bucket string, destLocalDir string, sourceAWSDir string) {
	query := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region)},
	)
	if err != nil {
		fmt.Println("Unable to connect to AWS", err)
		os.Exit(2)
	}
	svc := s3.New(sess)

	truncatedListing := true

	for truncatedListing {
		resp, err := svc.ListObjectsV2(query)

		if err != nil {
			fmt.Println(err.Error())
			return
		}
		getObjectsAll(resp, svc, bucket, destLocalDir, sourceAWSDir)
		query.ContinuationToken = resp.NextContinuationToken
		truncatedListing = *resp.IsTruncated
	}
}

var downloadedFileCount = 0

func getObjectsAll(bucketObjectsList *s3.ListObjectsV2Output, s3Client *s3.S3, bucket string, destLocalDir string, sourceAWSDir string) {
	for _, key := range bucketObjectsList.Contents {
		// fmt.Println(*key.Key)
		destFilename := *key.Key

		if !strings.HasPrefix(*key.Key, sourceAWSDir) {
			continue
		}

		if strings.HasSuffix(*key.Key, "/") {
			fmt.Println("Directory Found")
			continue
		}
		downloadedFileCount++
		if strings.Contains(*key.Key, "/") {
			var dirTree string

			s3FileFullPathList := strings.Split(*key.Key, "/")
			// fmt.Println("destFilename " + destFilename)
			for _, dir := range s3FileFullPathList[:len(s3FileFullPathList)-1] {
				dirTree += "/" + dir
			}
			os.MkdirAll(destLocalDir+"/"+dirTree, 0775)
		}
		out, err := s3Client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    key.Key,
		})
		if err != nil {
			log.Fatal(err)
		}
		destFilePath := destLocalDir + destFilename
		destFile, err := os.Create(destFilePath)
		if err != nil {
			log.Fatal(err)
		}
		bytes, err := io.Copy(destFile, out.Body)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("File %s of size %d bytes downloaded\n", destFilePath, bytes)
		out.Body.Close()
		destFile.Close()
	}
}
