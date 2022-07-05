package directory

import (
	"os"
	"path"
	"path/filepath"

	"playus/server-backup/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type Restore struct {
	Key       string
	Secret    string
	Region    string
	Endpoint  string
	Directory string
	Prefix    string
	Bucket    string
}

func NewRestore(directory string, bucket string, prefix string) *Restore {
	worker := &Restore{
		Key:       config.Conf.Get("dirbackup.key").(string),
		Secret:    config.Conf.Get("dirbackup.secret").(string),
		Region:    config.Conf.Get("dirbackup.region").(string),
		Endpoint:  config.Conf.Get("dirbackup.endpoint").(string),
		Bucket:    bucket,
		Prefix:    prefix,
		Directory: directory,
	}
	return worker
}

func (restore *Restore) RestoreBackup() {
	// create new aws session
	session, err := session.NewSession(&aws.Config{
		Region:      aws.String(restore.Region),
		Credentials: credentials.NewStaticCredentials(restore.Key, restore.Secret, ""),
		Endpoint:    aws.String(restore.Endpoint),
	})
	if checkErr(err) {
		return
	}

	// Create S3 service client
	s3Client := s3.New(session)
	if s3Client == nil {
		return
	}

	uploader := s3manager.NewUploader(session)
	if uploader == nil {
		return
	}
	downloader := s3manager.NewDownloader(session)
	if downloader == nil {
		return
	}

	util := NewS3Util(restore.Bucket, restore.Prefix, restore.Directory, s3Client, uploader, downloader)
	workQueue := NewWorkerQueue()
	for util.HasMore() {
		if workQueue.Size() >= 5 { // TODO: Allow to configure workers
			workQueue.DoWork()
		}
		for _, next := range *util.GetNextPage() {
			if next == nil {
				continue
			}
			remotePath := *next.Key
			extractedSuffix, err := util.ExtractTargetSuffix(remotePath)
			if checkErr(err) {
				continue
			}
			suffix := *extractedSuffix
			targetPath := filepath.Join(restore.Directory, suffix)
			if !CheckFileExists(targetPath) {
				if workQueue.Size() >= 5 { // TODO: Allow to configure workers
					workQueue.DoWork()
				}
				// file does not exist, delete from remote
				parentDir := path.Dir(targetPath)
				if !isDirectory(parentDir) {
					err := os.MkdirAll(parentDir, os.ModePerm)
					if checkErr(err) {
						continue
					}

				}
				workQueue.Add(NewDownloadWorker(remotePath, targetPath, *util))
			}
		}
	}
	if workQueue.Size() > 0 { // TODO: Allow to configure workers
		workQueue.DoWork()
	}
}
