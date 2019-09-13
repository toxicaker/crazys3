package pkg

type MigrationRequest struct {
	Files        *S3File
	SourceBucket string
	DestBucket   string
	DestFileName string
}
