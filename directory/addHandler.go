package directory

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"sort"
	"time"

	ignore "github.com/sabhiram/go-gitignore"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type AddHandler struct {
	Bucket          string
	Dir             string
	Prefix          string
	util            *S3Util
	dailyRotation   int
	weeklyRotation  int
	monthlyRotation int
	IgnoreObject    *ignore.GitIgnore
}

func NewAddHandler(bucket string, prefix string, dir string, s3Client *s3.S3, uploader *s3manager.Uploader, downloader *s3manager.Downloader, ignoreObject *ignore.GitIgnore, dailyRotation int, weeklyRotation int, monthlyRotation int) *AddHandler {
	return &AddHandler{
		Bucket:          bucket,
		Dir:             dir,
		Prefix:          prefix,
		util:            NewS3Util(bucket, prefix, dir, s3Client, uploader, downloader),
		dailyRotation:   dailyRotation,
		weeklyRotation:  weeklyRotation,
		monthlyRotation: monthlyRotation,
		IgnoreObject:    ignoreObject,
	}
}

func (handler *AddHandler) Handle() {
	fmt.Printf("Starting add handler for bucket %s in directory %s \n", handler.Bucket, handler.Dir)

	handler.handleDailyRotation()
	handler.handleRotations()
}

func (handler *AddHandler) handleRotations() {
	handler.handleRotation(WEEKLY, 7)
	handler.handleRotation(MONTHLY, 30)
}

func (handler *AddHandler) handleRotation(key string, days int) {
	previous := handler.util.GetTopDirectories(key)
	previousList := []dirDate{}
	if len(previous) > 0 {
		for _, next := range previous {
			dayTime, _ := time.Parse(RFC3339NoTime, next)
			previousList = append(previousList, dirDate{Value: next, DayTime: dayTime})
		}
	}

	sort.Sort(dirDateList(previousList)) // asc order

	if len(previousList) > 0 {
		now := time.Now()
		lastDate := previousList[(len(previousList) - 1)]
		diff := now.Sub(lastDate.DayTime)
		elapsedDays := int(diff.Hours() / 24)
		if elapsedDays > days {
			// create a new entry for the month
			// next run of removeHandler deletes based on rotation option
			handler.uploadDirectory(key)
		}
	} else {
		handler.uploadDirectory(key)
	}
}

func (handler *AddHandler) handleDailyRotation() {
	handler.uploadDirectory(DAILY)
}

func (handler *AddHandler) uploadDirectory(rotation string) {
	_, err := ioutil.ReadDir(handler.Dir)
	if err != nil {
		return
	}
	queue := NewQueue()
	queue.Enqueue(handler.Dir)

	now := time.Now()
	targetPrefix := fmt.Sprintf("%s/%s/", rotation, now.Format(RFC3339NoTime))

	for !queue.Empty() {
		nextDir, err := queue.Dequeue()
		if err != nil {
			return
		}
		entries, err := ioutil.ReadDir(nextDir)
		if err != nil {
			return
		}
		if checkErr(err) {
			continue
		}
		for _, entry := range entries {
			absPath := filepath.Join(nextDir, entry.Name())
			if handler.IgnoreObject != nil {
				if handler.IgnoreObject.MatchesPath(absPath) {
					continue
				}
			}

			if entry.IsDir() {
				queue.Enqueue(absPath)
				continue
			}
			checkSum := fileSha256(absPath)
			rel, err := filepath.Rel(handler.Dir, absPath)
			if checkErr(err) {
				continue
			}
			targetKey := handler.Prefix + "/" + targetPrefix + rel
			objectInfo := handler.util.ObjectExists(targetKey, checkSum)
			if !objectInfo.exists || !objectInfo.sameCheckSum {
				if objectInfo.exists {
					handler.util.DeleteFile(targetKey)
				}
				handler.util.UploadFile(absPath, targetKey)
			}
		}
	}
}
