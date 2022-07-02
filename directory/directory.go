package directory

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"playus/server-backup/config"
	"strings"
	"time"

	ignore "github.com/sabhiram/go-gitignore"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const RFC3339NoTime = "2006-01-02" // parse date format
const DAILY = "daily"
const WEEKLY = "weekly"
const MONTHLY = "monthly"
const SHA256 = "Checksumsha256"

/**
 * Allow sort of date directory names
 * Conform sort.Interface
 */
type dirDate struct {
	Value   string
	DayTime time.Time
}
type dirDateList []dirDate

func (dirDate dirDateList) Len() int {
	return len(dirDate)
}
func (dirDate dirDateList) Less(i, j int) bool {
	return dirDate[i].DayTime.Before(dirDate[j].DayTime)
}
func (dirDate dirDateList) Swap(i, j int) {
	dirDate[i], dirDate[j] = dirDate[j], dirDate[i]
}

type BackupDirectories struct {
	Bucket      string
	Prefix      string
	Directories []string
}

type DirectoryBackupWorker struct {
	Key             string
	Secret          string
	Region          string
	Endpoint        string
	Dirs            []BackupDirectories
	DailyRotation   int
	WeeklyRotation  int
	MonthlyRotation int
	Running         bool
	IgnoreObject    *ignore.GitIgnore
}

var (
	Worker = newDirectoryBackupWorker()
)

func newDirectoryBackupWorker() *DirectoryBackupWorker {
	object, err := ignore.CompileIgnoreFile(config.Conf.Get("dirbackup.ignoreFile").(string))
	checkErr(err)
	worker := &DirectoryBackupWorker{
		Key:             config.Conf.Get("dirbackup.key").(string),
		Secret:          config.Conf.Get("dirbackup.secret").(string),
		Region:          config.Conf.Get("dirbackup.region").(string),
		Endpoint:        config.Conf.Get("dirbackup.endpoint").(string),
		Dirs:            getDirectories(config.Conf.Get("dirbackup.dirs").(string)),
		DailyRotation:   int(config.Conf.Get("dirbackup.dailyrotation").(int64)),
		WeeklyRotation:  int(config.Conf.Get("dirbackup.weeklyrotation").(int64)),
		MonthlyRotation: int(config.Conf.Get("dirbackup.monthlyrotation").(int64)),
		Running:         false,
		IgnoreObject:    object,
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

			addHandler := NewAddHandler(targetBucketName, targetPrefix, nextDir, s3Client, uploader, downloader, worker.IgnoreObject, worker.DailyRotation, worker.WeeklyRotation, worker.MonthlyRotation)
			addHandler.Handle()

			removeHandler := NewRemoveHandler(targetBucketName, targetPrefix, nextDir, s3Client, uploader, downloader, worker.DailyRotation, worker.WeeklyRotation, worker.MonthlyRotation)
			removeHandler.Handle()
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
		bucket := nextTarget[0:(separator)]

		remaining := nextTarget[(separator + 1):]
		separator = strings.Index(remaining, "|")
		if separator < 0 {
			fmt.Printf("Invalid directory %s no bucket prefix specified", nextTarget)
		}
		prefix := remaining[0:(separator)]
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
			targetBucket.Directories = append(targetBucket.Directories, targetDir)
			buckets[bucketKey] = targetBucket
		} else {
			targetBucket.Directories = append(targetBucket.Directories, targetDir)
		}
	}
	result := []BackupDirectories{}
	for _, val := range buckets {
		result = append(result, val)
	}
	return result
}

// package level

func isDirectory(dir string) bool {
	exists := CheckFileExists(dir)
	if !exists {
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
		fmt.Println(fmt.Printf("[ERROR] %s", err))
		return true
	}
	return false
}

func notRunning(worker *DirectoryBackupWorker) {
	worker.Running = false
}

func CheckFileExists(filePath string) bool {
	_, error := os.Stat(filePath)
	return !errors.Is(error, os.ErrNotExist)
}

func FileSha256(filePath string) *string {
	f, err := os.Open(filePath)
	if checkErr(err) {
		return nil
	}

	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		checkErr(err)
		return nil
	}
	val := fmt.Sprintf("%x", h.Sum(nil))
	return &val
}

func prettyEncode(data interface{}, out io.Writer) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "    ")
	if err := enc.Encode(data); err != nil {
		return err
	}
	return nil
}
