package pkg

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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

// list all buckets in the account. region doesn't impact the result
func (manager *S3Manager) ListBuckets() ([]*s3.Bucket, error) {
	res, err := manager.s3cli.ListBuckets(&s3.ListBucketsInput{})
	return res.Buckets, err
}

// get bucket's region
func (manager *S3Manager) GetBucketRegion(bucket string) (string, error) {
	return s3manager.GetBucketRegionWithClient(context.Background(), manager.s3cli, bucket)
}

func (manager *S3Manager) GetFileAcls(bucket string, fileName string) ([]*s3.Grant, error) {
	input := &s3.GetObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileName),
	}
	res, err := manager.s3cli.GetObjectAcl(input)
	return res.Grants, err
}

func (manager *S3Manager) PutFileAcls(bucket string, fileName string, grants []*s3.Grant) ([]*s3.Grant, error) {
	input := &s3.PutObjectAclInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(fileName),
	}
	res, err := manager.s3cli.GetObjectAcl(input)
	return res.Grants, err
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
					GLogger.Warning("Exception in handling file %v of bucket %v, reason: %v", s3file.Name, bucketName, e)
				}
			}
			return !lastPage
		})
	return err
}

// Copy a file object from source bucket to destination
// It CAN preserve ACLS
// wiki: https://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html
func (manager *S3Manager) CopyFile(sourceBucket string, sourceFileName string, destBucket string, destFileName string) error {
	input := &s3.CopyObjectInput{
		Bucket:     aws.String(destBucket),
		CopySource: aws.String("/" + sourceBucket + "/" + sourceFileName),
		Key:        aws.String(destFileName),
	}
	res, err := manager.s3cli.CopyObject(input)
	if err != nil {
		return err
	}
	GLogger.Debug("copied file %v to %v, res=%v", sourceBucket+"/"+sourceFileName, destBucket+"/"+destFileName, res)
	return nil
}
