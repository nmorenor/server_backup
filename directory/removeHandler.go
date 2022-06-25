package directory

import (
	"fmt"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type RemoveHandler struct {
	Bucket string
	Dir    string
	Prefix string
	util   *S3Util
}

func NewRemoveHandler(bucket string, prefix string, dir string, s3Client *s3.S3, uploader *s3manager.Uploader, downloader *s3manager.Downloader) *RemoveHandler {
	return &RemoveHandler{
		Bucket: bucket,
		Dir:    dir,
		Prefix: prefix,
		util:   NewS3Util(bucket, prefix, dir, s3Client, uploader, downloader),
	}
}

func (handler *RemoveHandler) Handle() {
	fmt.Printf("Starting remove handler for bucket %s in directory %s", handler.Bucket, handler.Dir)
}
