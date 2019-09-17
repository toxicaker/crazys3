package pkg

type MigrationRequest struct {
	File         *S3File
	SourceBucket string
	DestBucket   string
	DestFileName string
	Finished     bool
}

type RestorationRequest struct {
	File     *S3File
	Finished bool
	Bucket   string
	Days     int64
	Speed    string
}

type RecoveryRequest struct {
	File     *S3File
	Finished bool
	Bucket   string
}

type S3InfoRequest struct {
	Profile   string
	Region    string
	AwsKey    string
	AwsSecret string
}
