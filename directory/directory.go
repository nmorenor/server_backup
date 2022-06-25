package directory

import (
	"fmt"
	"os"
	"playus/server-backup/config"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type BackupDirectories struct {
	Bucket      string
	Prefix      string
	Directories []string
}

type DirectoryBackupWorker struct {
	Key      string
	Secret   string
	Region   string
	Endpoint string
	Dirs     []BackupDirectories
	Running  bool
}

var (
	Worker = newDirectoryBackupWorker()
)

func newDirectoryBackupWorker() *DirectoryBackupWorker {
	worker := &DirectoryBackupWorker{
		Key:      config.Conf.Get("dirbackup.key").(string),
		Secret:   config.Conf.Get("dirbackup.secret").(string),
		Region:   config.Conf.Get("dirbackup.region").(string),
		Endpoint: config.Conf.Get("dirbackup.endpoint").(string),
		Dirs:     getDirectories(config.Conf.Get("dirbackup.dirs").(string)),
		Running:  false,
	}
	return worker
}

func (worker *DirectoryBackupWorker) DoBackup() {
	if worker.Running {
		fmt.Printf("Dir backup already running")
		return
	}
	defer notRunning(worker)

	// create new aws session
	session, err := session.NewSession(&aws.Config{
		Region:      aws.String(worker.Region),
		Credentials: credentials.NewStaticCredentials(worker.Key, worker.Secret, ""),
		Endpoint:    aws.String(worker.Endpoint),
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

	for _, nextBucket := range worker.Dirs {
		targetBucketName := nextBucket.Bucket
		targetPrefix := nextBucket.Prefix
		if len(nextBucket.Directories) <= 0 {
			break
		}
		for j := range nextBucket.Directories {
			nextDir := nextBucket.Directories[j]

			removeHandler := NewRemoveHandler(targetBucketName, targetPrefix, nextDir, s3Client, uploader, downloader)
			removeHandler.Handle()

			addHandler := NewAddHandler(targetBucketName, targetPrefix, nextDir, s3Client, uploader, downloader)
			addHandler.Handle()
		}
	}
}

func getDirectories(dirs string) []BackupDirectories {
	dirs = strings.Replace(dirs, " , ", ",", -1)
	dirs = strings.Replace(dirs, ", ", ",", -1)
	dirs = strings.Replace(dirs, " ,", ",", -1)
	targetDirs := strings.Split(dirs, ",")
	targetDirs = removeDuplicates(targetDirs)

	buckets := make(map[string]BackupDirectories)

	for n := range targetDirs {
		nextTarget := targetDirs[n]
		separator := strings.Index(nextTarget, "|")
		if separator < 0 {
			fmt.Printf("Invalid directory %s no bucket specified", nextTarget)
		}
		bucket := nextTarget[0:(separator - 1)]

		remaining := nextTarget[(separator + 1):]
		separator = strings.Index(remaining, "|")
		if separator < 0 {
			fmt.Printf("Invalid directory %s no bucket prefix specified", nextTarget)
		}
		prefix := remaining[0:(separator - 1)]
		targetDir := remaining[(separator + 1):]
		if !isDirectory(targetDir) {
			continue
		}

		bucketKey := fmt.Sprintf("%s:%s", bucket, prefix)
		targetBucket, exists := buckets[bucketKey]
		if !exists {
			targetBucket = BackupDirectories{
				Bucket:      bucket,
				Prefix:      prefix,
				Directories: []string{},
			}
			buckets[bucket] = targetBucket
		}
		targetBucket.Directories = append(targetBucket.Directories, targetDir)
	}
	result := []BackupDirectories{}
	for _, val := range buckets {
		result = append(result, val)
	}
	return result
}

func isDirectory(dir string) bool {
	exists, err := fileExist(dir)
	if !exists || err != nil {
		return false
	}

	si, err := os.Stat(dir)

	if err != nil {
		checkErr(err)
		return false
	}

	if !si.IsDir() {
		fmt.Printf("dir: %s is not a directory", dir)
		return false
	}
	return true
}

func fileExist(file string) (bool, error) {
	stat, err := os.Stat(file)
	if err != nil && os.IsNotExist(err) {
		checkErr(err)
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return stat != nil, nil
}

func removeDuplicates(elements []string) []string {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	result := []string{}

	for v := range elements {
		if encountered[elements[v]] {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			encountered[elements[v]] = true
			// Append to result slice.
			result = append(result, elements[v])
		}
	}
	// Return the new slice.
	return result
}

func checkErr(err error) bool {
	if err != nil {
		fmt.Printf("[ERROR] %s", err)
		return true
	}
	return false
}

func notRunning(worker *DirectoryBackupWorker) {
	worker.Running = false
}
