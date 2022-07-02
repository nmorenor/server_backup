package typesensebackup

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"playus/server-backup/config"
	"playus/server-backup/directory"

	"github.com/typesense/typesense-go/typesense"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type StackNode struct {
	Value    string
	nextNode *StackNode
}

type Stack struct {
	First *StackNode
}

func (stack *Stack) Add(value string) {
	node := &StackNode{
		Value:    value,
		nextNode: nil,
	}
	if stack.First == nil {
		stack.First = node
	} else {
		oldFirst := stack.First
		node.nextNode = oldFirst
		stack.First = node
	}
}

func (stack *Stack) HasMore() bool {
	return stack.First != nil
}

func (stack *Stack) GetNext() string {
	oldFirst := stack.First
	stack.First = oldFirst.nextNode
	return oldFirst.Value
}

type TypesenseBackup struct {
	Key             string
	Secret          string
	Region          string
	Endpoint        string
	TargetDir       string
	Bucket          string
	BucketPrefix    string
	DailyRotation   int
	WeeklyRotation  int
	MonthlyRotation int
	Running         bool
	TypeSenseClient *typesense.Client
}

var (
	Worker = newTypesenseBackupWorker()
)

func newTypesenseBackupWorker() *TypesenseBackup {
	typesenseClient := typesense.NewClient(
		typesense.WithServer(config.Conf.Get("typesensebackup.typesenseUrl").(string)),
		typesense.WithAPIKey(config.Conf.Get("typesensebackup.typesenseApiKey").(string)))
	worker := &TypesenseBackup{
		Key:             config.Conf.Get("typesensebackup.key").(string),
		Secret:          config.Conf.Get("typesensebackup.secret").(string),
		Region:          config.Conf.Get("typesensebackup.region").(string),
		Endpoint:        config.Conf.Get("typesensebackup.endpoint").(string),
		Bucket:          config.Conf.Get("typesensebackup.bucket").(string),
		BucketPrefix:    config.Conf.Get("typesensebackup.bucketPrefix").(string),
		TargetDir:       config.Conf.Get("typesensebackup.targetDir").(string),
		DailyRotation:   int(config.Conf.Get("typesensebackup.dailyrotation").(int64)),
		WeeklyRotation:  int(config.Conf.Get("typesensebackup.weeklyrotation").(int64)),
		MonthlyRotation: int(config.Conf.Get("typesensebackup.monthlyrotation").(int64)),
		Running:         false,
		TypeSenseClient: typesenseClient,
	}

	return worker
}

func (worker *TypesenseBackup) DoBackup() {
	if worker.Running {
		return
	}
	if !directory.CheckFileExists(worker.TargetDir) {
		err := os.MkdirAll(worker.TargetDir, os.ModePerm)
		if checkErr(err) {
			return
		}
	}
	targetSnapshot := fmt.Sprintf("%s/%s", worker.TargetDir, "typesense-snapshot")
	success, err := worker.TypeSenseClient.Operations().Snapshot(targetSnapshot)
	if checkErr(err) {
		return
	}
	if success {
		targetFile := fmt.Sprintf("%s/typesense-backup.tgz", worker.TargetDir)
		worker.compressDirectory(targetSnapshot, targetFile)
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

		addHandler := directory.NewAddHandler(worker.Bucket, worker.Key, worker.TargetDir, s3Client, uploader, downloader, nil, worker.DailyRotation, worker.WeeklyRotation, worker.MonthlyRotation)
		addHandler.Handle()

		removeHandler := directory.NewRemoveHandler(worker.Bucket, worker.Key, worker.TargetDir, s3Client, uploader, downloader, worker.DailyRotation, worker.WeeklyRotation, worker.MonthlyRotation)
		removeHandler.Handle()
	}

}

func (worker *TypesenseBackup) compressDirectory(targetDir string, targetFile string) {
	file, errcreate := os.Create(targetFile)

	if checkErr(errcreate) {
		return
	}
	defer file.Close()
	// set up the gzip writer
	gw := gzip.NewWriter(file)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	_, err := ioutil.ReadDir(targetDir)
	if err != nil {
		return
	}
	dirsToDelete := Stack{}

	queue := directory.NewQueue()
	queue.Enqueue(targetDir)
	dirsToDelete.Add(targetDir)

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

			if entry.IsDir() {
				queue.Enqueue(absPath)
				dirsToDelete.Add(absPath)
				continue
			}
			rel, err := filepath.Rel(targetDir, absPath)
			if checkErr(err) {
				continue
			}
			if errcompress := Compress(tw, absPath, rel); errcompress != nil {
				fmt.Printf("error to compress file: %s \n", err)
				checkErr(errcompress)
			}
		}
	}
	for dirsToDelete.HasMore() {
		dir := dirsToDelete.GetNext()
		err = os.Remove(dir)
		checkErr(err)
	}
}

func Compress(tw *tar.Writer, p string, targetKey string) error {
	file, err := os.Open(p)
	if err != nil {
		return err
	}
	defer file.Close()
	if stat, err := file.Stat(); err == nil {
		// now lets create the header as needed for this file within the tarball
		header := new(tar.Header)
		header.Name = targetKey
		header.Size = stat.Size()
		header.Mode = int64(stat.Mode())
		header.ModTime = stat.ModTime()
		// write the header to the tarball archive
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		// copy the file data to the tarball
		if _, err := io.Copy(tw, file); err != nil {
			return err
		}

		// Removing the original file after zipping it
		err = os.Remove(p)

		if err != nil {
			fmt.Println(err)
			return err
		}
	}
	return nil
}

func checkErr(err error) bool {
	if err != nil {
		fmt.Println(fmt.Printf("[ERROR] %s", err))
		return true
	}
	return false
}
