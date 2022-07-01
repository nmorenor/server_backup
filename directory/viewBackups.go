package directory

import (
	"bytes"
	"fmt"
	"strings"

	"playus/server-backup/config"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type BackupView struct {
	Key      string
	Secret   string
	Region   string
	Endpoint string
	Bucket   string
	MaxKeys  int64
}

type BackupKeys struct {
	Keys []BackupKey `json:"keys"`
}

type BackupKey struct {
	Name     string       `json:"name"`
	Children *[]BackupKey `json:"children"`
}

func NewBackupView(bucket string) *BackupView {
	backupView := &BackupView{
		Key:      config.Conf.Get("dirbackup.key").(string),
		Secret:   config.Conf.Get("dirbackup.secret").(string),
		Region:   config.Conf.Get("dirbackup.region").(string),
		Endpoint: config.Conf.Get("dirbackup.endpoint").(string),
		Bucket:   bucket,
		MaxKeys:  int64(100),
	}
	return backupView
}

func (view *BackupView) ViewBackup() {
	session, err := session.NewSession(&aws.Config{
		Region:      aws.String(view.Region),
		Credentials: credentials.NewStaticCredentials(view.Key, view.Secret, ""),
		Endpoint:    aws.String(view.Endpoint),
	})
	if checkErr(err) {
		return
	}

	// Create S3 service client
	s3Client := s3.New(session)
	if s3Client == nil {
		return
	}
	topKeys := view.getTopDirectories(s3Client, "")
	keys := []BackupKey{}
	for _, next := range topKeys {
		keys = append(keys, BackupKey{
			Name:     next,
			Children: &[]BackupKey{},
		})
	}

	for _, next := range keys {
		children := []BackupKey{}
		view.getChildren(s3Client, DAILY, fmt.Sprintf("%s/%s/", next.Name, DAILY), &children)
		view.getChildren(s3Client, WEEKLY, fmt.Sprintf("%s/%s/", next.Name, WEEKLY), &children)
		view.getChildren(s3Client, MONTHLY, fmt.Sprintf("%s/%s/", next.Name, MONTHLY), &children)
		(*next.Children) = children
	}

	result := BackupKeys{
		Keys: keys,
	}
	var buffer bytes.Buffer
	err = prettyEncode(result, &buffer)
	if checkErr(err) {
		return
	}
	fmt.Println(buffer.String())
}

func (view *BackupView) getChildren(client *s3.S3, parent string, path string, target *[]BackupKey) {
	children := *target
	pathChildrenKeys := view.getTopDirectories(client, path)
	if len(pathChildrenKeys) > 0 {
		pathChildren := []BackupKey{}
		for _, nextKey := range pathChildrenKeys {
			pathChildren = append(pathChildren, BackupKey{
				Name:     nextKey,
				Children: nil,
			})
		}
		(*target) = append(children, BackupKey{
			Name:     parent,
			Children: &pathChildren,
		})
	}
}

func (view *BackupView) getTopDirectories(client *s3.S3, path string) []string {
	dirs := &map[string]bool{}
	// targetPath := fmt.Sprintf("%s/%s", util.Prefix, path)
	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(view.Bucket),
		Prefix:  aws.String(path),
		MaxKeys: &view.MaxKeys,
	}
	view.processTopListInput(client, path, dirs, listInput)

	result := []string{}
	for key := range *dirs {
		result = append(result, key)
	}
	return result
}

func (view *BackupView) processTopListInput(client *s3.S3, targetPath string, dirs *map[string]bool, listInput *s3.ListObjectsV2Input) {
	resp, err := client.ListObjectsV2(listInput)
	if checkErr(err) {
		return
	}
	for listInput != nil {
		for _, nextObject := range resp.Contents {
			if nextObject == nil {
				continue
			}
			objectKey := (*nextObject.Key)
			suffix := objectKey[len(targetPath):]
			index := strings.Index(suffix, "/")
			if index < 0 {
				// not a directory
				continue
			}
			dirName := suffix[0:index]
			_, exists := (*dirs)[dirName]

			if exists {
				// already on the map
				continue
			}
			(*dirs)[dirName] = true
		}
		if resp.ContinuationToken == nil {
			// end the loop
			listInput = nil
		} else {
			listInput := &s3.ListObjectsV2Input{
				Bucket:            aws.String(view.Bucket),
				Prefix:            aws.String(targetPath),
				MaxKeys:           &view.MaxKeys,
				ContinuationToken: resp.ContinuationToken,
			}
			resp, err = client.ListObjectsV2(listInput)
			if checkErr(err) {
				return
			}
		}
	}
}
