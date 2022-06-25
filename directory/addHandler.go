package directory

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type AddHandler struct {
	Bucket string
	Dir    string
	Prefix string
	util   *S3Util
}

func NewAddHandler(bucket string, prefix string, dir string, s3Client *s3.S3, uploader *s3manager.Uploader, downloader *s3manager.Downloader) *AddHandler {
	return &AddHandler{
		Bucket: bucket,
		Dir:    dir,
		Prefix: prefix,
		util:   NewS3Util(bucket, prefix, dir, s3Client, uploader, downloader),
	}
}

func (handler *AddHandler) Handle() {
	fmt.Printf("Starting add handler for bucket %s in directory %s", handler.Bucket, handler.Dir)
	entries, err := ioutil.ReadDir(handler.Dir)
	if err != nil {
		return
	}
	queue := NewQueue()
	queue.Enqueue(handler.Dir)

	now := time.Now()

	targetPrefix := "daily/" + now.Format("2006-01-02") + "/"

	for !queue.Empty() {
		nextDir, err := queue.Dequeue()
		if checkErr(err) {
			continue
		}
		for _, entry := range entries {
			absPath := filepath.Join(nextDir, entry.Name())

			if entry.IsDir() {
				queue.Enqueue(absPath)
				continue
			}

			rel, err := filepath.Rel(handler.Dir, absPath)
			if checkErr(err) {
				continue
			}
			targetKey := handler.Prefix + "/" + targetPrefix + rel
			if !handler.util.ObjectExists(targetKey) {
				handler.util.UploadFile(absPath, targetKey)
			}
		}
	}

}
