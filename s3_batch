package s3_upload

import (
	"context"
	"crypto/md5"
	"encoding/base64"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/rs/zerolog"
)

// S3Uploader AWS S3 uploader
type S3Uploader struct {
	logger   zerolog.Logger
	uploader *s3manager.Uploader
	Region   string
	Bucket   string
	Key      string
	FileName string
}

// directoryIterator iterates through files and directories to be uploaded
// to S3.
type directoryIterator struct {
	filePaths []string
	bucket    string
	key       string
	next      struct {
		path string
		f    *os.File
	}
	err error
	ctx context.Context
}

// Upload data to AWS S3 bucket
func (s3 *S3Uploader) Upload() error {
	startTime := time.Now()
	fName := filepath.Base(s3.FileName)
	file, err := os.Open(s3.FileName)
	if err != nil {
		return err
	}
	defer s3.deleteLocalFile()
	defer file.Close()

	_, err = s3.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s3.Bucket),                    // Bucket to be used
		Key:    aws.String(filepath.Join(s3.Key, fName)), // Name of the file to be saved
		Body:   file,                                     // File
	})
	if err != nil {
		return err
	}
	endTime := time.Now()
	s3.logger.Info().Str("bucket", s3.Bucket).Float64("time", endTime.Sub(startTime).Seconds()).Msg("Successfully uploaded")
	return nil
}

func (s3 *S3Uploader) deleteLocalFile() error {
	return os.Remove(s3.FileName)
}

func (s3 *S3Uploader) UploadDir(ctx context.Context) (error, []*s3manager.UploadOutput, []string, string) {

	iter := newDirectoryIterator(s3.Bucket, s3.Key, s3.FileName, ctx)

	// Updated the original `*s3manager.Uploader.UploadWithIterator` to address multi upload `checksum` addition
	// in the manifest file
	// Uploading collected data
	err, upOutputs, chkSums, objKey := UploadWithIterator(s3.uploader, aws.BackgroundContext(), iter)
	if err != nil {
		s3.logger.Error().Err(err).Msg("Failed to upload collected data")
		return err, nil, nil, ""
	}
	s3.logger.Info().Str("fileName", s3.FileName).Str("region", s3.Region).Msg("Successfully uploaded")
	return err, upOutputs, chkSums, objKey
}

// NewDirectoryIterator creates and returns a new BatchUploadIterator
func newDirectoryIterator(bucket string, key string, dir string, ctx context.Context) s3manager.BatchUploadIterator {
	paths := []string{}
	filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		// We care only about files, not directories
		if !info.IsDir() {
			paths = append(paths, path)
		}
		return nil
	})

	return &directoryIterator{
		filePaths: paths,
		bucket:    bucket,
		key:       key,
		ctx:       ctx,
	}
}

// Next opens the next file and stops iteration if it fails to open
// a file.
func (iter *directoryIterator) Next() bool {
	if len(iter.filePaths) == 0 {
		iter.next.f = nil
		return false
	}

	f, err := os.Open(iter.filePaths[0])
	iter.err = err

	iter.next.f = f
	iter.next.path = iter.filePaths[0]

	iter.filePaths = iter.filePaths[1:]
	return true && iter.Err() == nil
}

// Err returns an error that was set during opening the file
func (iter *directoryIterator) Err() error {
	return iter.err
}

// UploadObject returns a BatchUploadObject and sets the After field to
// close the file.
func (iter *directoryIterator) UploadObject() s3manager.BatchUploadObject {
	f := iter.next.f
	fName := filepath.Base(iter.next.path)

	encSum := iter.getChecksum()

	f.Seek(0, io.SeekStart)

	return s3manager.BatchUploadObject{
		Object: &s3manager.UploadInput{
			Bucket:     &iter.bucket,
			Key:        aws.String(filepath.Join(iter.key, fName)),
			Body:       f,
			ContentMD5: aws.String(encSum),
		},
		// After was introduced in version 1.10.7
		After: func() error {
			return f.Close()
		},
	}
}

func (iter *directoryIterator) getChecksum() string {
	f := iter.next.f
	hash := md5.New()
	io.Copy(hash, f)
	sum := hash.Sum(nil)
	encSum := base64.StdEncoding.EncodeToString(sum)
	return encSum
}

func UploadWithIterator(u *s3manager.Uploader, ctx aws.Context, iter s3manager.BatchUploadIterator, opts ...func(uploader *s3manager.Uploader)) (error, []*s3manager.UploadOutput, []string, string) {
	var errs []s3manager.Error
	var chkSums []string
	var upOutputs []*s3manager.UploadOutput
	var objKey string
	for iter.Next() {
		object := iter.UploadObject()
		if f, ok := object.Object.Body.(*os.File); ok {
			f.Seek(0, io.SeekStart)
		}
		if upOut, err := u.UploadWithContext(ctx, object.Object, opts...); err != nil {
			s3Err := s3manager.Error{
				OrigErr: err,
				Bucket:  object.Object.Bucket,
				Key:     object.Object.Key,
			}
			errs = append(errs, s3Err)
		} else {
			objKey = *object.Object.Key
			upOut.Location = objKey
			upOutputs = append(upOutputs, upOut)
			chkSums = append(chkSums, *object.Object.ContentMD5)
		}

		if object.After == nil {
			continue
		}

		if err := object.After(); err != nil {
			s3Err := s3manager.Error{
				OrigErr: err,
				Bucket:  object.Object.Bucket,
				Key:     object.Object.Key,
			}

			errs = append(errs, s3Err)
		}

	}

	if len(errs) > 0 {
		return s3manager.NewBatchError("BatchedUploadIncomplete", "some objects have failed to upload.", errs), nil, nil, ""
	}

	return nil, upOutputs, chkSums, objKey
}
