package pkg

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"sync/atomic"
)

type S3Manager struct {
	s3cli   *s3.S3
	profile string
	region  string
}

type S3File struct {
	Id           int64
	BucketName   string
	Name         string
	Size         int64
	StorageClass string
}

// make sure you have ~/.aws/credentials
func NewS3Manager(region, profile string) (*S3Manager, error) {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewSharedCredentials("", profile),
	})
	if err != nil {
		return nil, err
	}
	manager := &S3Manager{
		s3cli:   s3.New(sess),
		profile: profile,
		region:  region,
	}
	return manager, nil
}

func (manager *S3Manager) HandleFiles(bucketName string, prefix string, handler func(file *S3File) error) error {
	param := &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	}
	var (
		Id      int64
		pageNum int
	)
	err := manager.s3cli.ListObjectsPages(param,
		func(page *s3.ListObjectsOutput, lastPage bool) bool {
			pageNum++
			for i := 0; i < len(page.Contents); i++ {
				atomic.AddInt64(&Id, 1)
				s3file := &S3File{
					Id:           Id,
					BucketName:   bucketName,
					Name:         *page.Contents[i].Key,
					Size:         *page.Contents[i].Size,
					StorageClass: *page.Contents[i].StorageClass,
				}
				e := handler(s3file)
				if e != nil {
					GLogger.Warning("Exception in handling file %v of bucket %v", s3file.Name, bucketName)
				}
			}
			return !lastPage
		})
	return err
}
