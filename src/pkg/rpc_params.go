package pkg

type MigrationRequest struct {
	File         *S3File
	SourceBucket string
	DestBucket   string
	DestFileName string
	Finished     bool
}
