package directory

import (
	"fmt"
	"path/filepath"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type RemoveHandler struct {
	Bucket          string
	Dir             string
	Prefix          string
	util            *S3Util
	dailyRotation   int
	weeklyRotation  int
	monthlyRotation int
}

func NewRemoveHandler(bucket string, prefix string, dir string, s3Client *s3.S3, uploader *s3manager.Uploader, downloader *s3manager.Downloader, dailyRotation int, weeklyRotation int, monthlyRotation int) *RemoveHandler {
	return &RemoveHandler{
		Bucket:          bucket,
		Dir:             dir,
		Prefix:          prefix,
		util:            NewS3Util(bucket, prefix, dir, s3Client, uploader, downloader),
		dailyRotation:   dailyRotation,
		weeklyRotation:  weeklyRotation,
		monthlyRotation: monthlyRotation,
	}
}

func (handler *RemoveHandler) Handle() {
	fmt.Printf("Starting remove handler for bucket %s in directory %s \n", handler.Bucket, handler.Dir)

	handler.handleFileSystemDeletions()
	handler.handleRotations()
}

/**
 * Delete files on remote that were deleted on file system
 */
func (handler *RemoveHandler) handleFileSystemDeletions() {
	// only sync todays dir
	targetDatePrefix := fmt.Sprintf("/%s/", time.Now().Format(RFC3339NoTime))
	targetPrefix := fmt.Sprintf("%s/daily%s", handler.util.Prefix, targetDatePrefix)
	prefixUtil := NewS3Util(handler.util.Bucket, targetPrefix, handler.Dir, handler.util.client, handler.util.uploader, handler.util.downloader)
	for prefixUtil.HasMore() {
		for _, next := range *prefixUtil.GetNextPage() {
			if next == nil {
				continue
			}
			remotePath := *next.Key
			extractedSuffix, err := prefixUtil.ExtractTargetSuffix(remotePath)
			if checkErr(err) {
				continue
			}
			suffix := *extractedSuffix
			targetPath := filepath.Join(handler.Dir, suffix)

			if !checkFileExists(targetPath) {
				// file does not exist, delete from remote
				prefixUtil.DeleteFile(remotePath)
			}
		}
	}
}

/**
 * Make sure we don't keep more backups from what was specified on config file
 */
func (handler *RemoveHandler) handleRotations() {
	handler.handleMaxRotation(DAILY, handler.dailyRotation)
	handler.handleMaxRotation(WEEKLY, handler.weeklyRotation)
	handler.handleMaxRotation(MONTHLY, handler.monthlyRotation)
}

func (handler *RemoveHandler) handleMaxRotation(key string, rotation int) {
	previous := handler.util.GetTopDirectories(key)
	previousList := []dirDate{}
	if len(previous) > 0 {
		for _, next := range previous {
			dayTime, _ := time.Parse(RFC3339NoTime, next)
			previousList = append(previousList, dirDate{Value: next, DayTime: dayTime})
		}
	}

	sort.Sort(dirDateList(previousList)) // asc order

	if len(previousList) > rotation {
		index := 0
		for index < rotation {
			next := previousList[index]
			handler.util.CleanFiles(fmt.Sprintf("%s/%s", key, next.Value))
		}
	}
}
