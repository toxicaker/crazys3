package pkg

type MigrationRequest struct {
	File         *S3File
	SourceBucket string
	DestBucket   string
	DestFileName string
	Finished     bool
}

type S3InfoRequest struct {
	Profile   string
	Region    string
	AwsKey    string
	AwsSecret string
}
