package directory

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type S3Util struct {
	Bucket           string
	Prefix           string
	client           *s3.S3
	listInput        *s3.ListObjectsV2Input
	uploader         *s3manager.Uploader
	downloader       *s3manager.Downloader
	listInputHasMore bool
	page             int
	maxKeys          int64
}

func NewS3Util(bucket string, prefix string, dir string, s3Client *s3.S3, uploader *s3manager.Uploader, downloader *s3manager.Downloader) *S3Util {
	return &S3Util{
		Bucket:           bucket,
		Prefix:           prefix,
		client:           s3Client,
		page:             0,
		maxKeys:          int64(1),
		listInput:        nil,
		listInputHasMore: false,
		uploader:         uploader,
		downloader:       downloader,
	}
}

func (util *S3Util) ResetPage() {
	util.page = 0
	util.listInput = nil
}

func (util *S3Util) IsStart() bool {
	return util.listInput == nil
}

func (util *S3Util) HasMore() bool {
	return util.IsStart() || util.listInputHasMore
}

func (util *S3Util) ObjectExists(keyFile string) bool {
	input := &s3.GetObjectInput{
		Bucket: aws.String(util.Bucket),
		Key:    aws.String(keyFile),
	}
	object, err := util.client.GetObject(input)
	return object != nil && err == nil
}

func (util *S3Util) GetNextPage() *[]*s3.Object {

	if util.listInput == nil {
		util.listInput = &s3.ListObjectsV2Input{
			Bucket:  aws.String(util.Bucket),
			Prefix:  aws.String(util.Prefix),
			MaxKeys: &util.maxKeys,
		}
	}
	resp, err := util.client.ListObjectsV2(util.listInput)
	if checkErr(err) {
		return nil
	}
	result := []*s3.Object{}
	for _, key := range resp.Contents {
		if key == nil {
			continue
		}
		result = append(result, key)
	}
	util.listInput.ContinuationToken = resp.NextContinuationToken
	util.listInputHasMore = *resp.IsTruncated

	return &result
}

// path without prefix
func (util *S3Util) GetTopDirectories(path string) []string {
	dirs := &map[string]bool{}
	targetPath := fmt.Sprintf("%s/%s", util.Prefix, path)
	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(util.Bucket),
		Prefix:  aws.String(targetPath),
		MaxKeys: &util.maxKeys,
	}
	util.processTopListInput(targetPath, dirs, listInput)

	result := []string{}
	for key := range *dirs {
		result = append(result, key)
	}
	return result
}

func (util *S3Util) processTopListInput(targetPath string, dirs *map[string]bool, listInput *s3.ListObjectsV2Input) {
	resp, err := util.client.ListObjectsV2(listInput)
	if checkErr(err) {
		return
	}
	for listInput != nil {
		for _, nextObject := range resp.Contents {
			if nextObject == nil {
				continue
			}
			objectKey := (*nextObject.Key)
			suffix := objectKey[len(targetPath)+1:]
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
				Bucket:            aws.String(util.Bucket),
				Prefix:            aws.String(targetPath),
				MaxKeys:           &util.maxKeys,
				ContinuationToken: resp.ContinuationToken,
			}
			resp, err = util.client.ListObjectsV2(listInput)
			if checkErr(err) {
				return
			}
		}
	}
}

func (util *S3Util) CleanFiles(path string) []string {
	dirs := &map[string]bool{}
	targetPath := fmt.Sprintf("%s/%s", util.Prefix, path)
	listInput := &s3.ListObjectsV2Input{
		Bucket:  aws.String(util.Bucket),
		Prefix:  aws.String(targetPath),
		MaxKeys: &util.maxKeys,
	}
	util.processCleanFiles(targetPath, dirs, listInput)

	result := []string{}
	for key := range *dirs {
		result = append(result, key)
	}
	return result
}

func (util *S3Util) processCleanFiles(targetPath string, dirs *map[string]bool, listInput *s3.ListObjectsV2Input) {
	resp, err := util.client.ListObjectsV2(listInput)
	if checkErr(err) {
		return
	}
	for listInput != nil {
		for _, nextObject := range resp.Contents {
			if nextObject == nil {
				continue
			}
			objectKey := (*nextObject.Key)
			err = util.DeleteFile(objectKey)
			if checkErr(err) {
				continue
			}
		}
		if resp.ContinuationToken == nil {
			// end the loop
			listInput = nil
		} else {
			listInput := &s3.ListObjectsV2Input{
				Bucket:            aws.String(util.Bucket),
				Prefix:            aws.String(targetPath),
				MaxKeys:           &util.maxKeys,
				ContinuationToken: resp.ContinuationToken,
			}
			resp, err = util.client.ListObjectsV2(listInput)
			if checkErr(err) {
				return
			}
		}
	}
}

func (util *S3Util) UploadFile(targetFile string, targetKey string) error {
	file, err := os.Open(targetFile)
	if checkErr(err) {
		return err
	}

	result, err := util.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(util.Bucket),
		Key:    aws.String(targetKey),
		Body:   file,
	})

	if checkErr(err) {
		return err
	}

	fmt.Println("[INFO] Upload successfully! Path of archive:" + result.Location)
	return nil
}

func (util *S3Util) DeleteFile(targetKey string) error {
	_, err := util.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(util.Bucket),
		Key:    aws.String(targetKey),
	})
	if checkErr(err) {
		return err
	}

	return nil
}

func (util *S3Util) DownloadFile(targetKey string, targetDir string) error {
	// all backups have 3 prefix ${dirPrefix/daily|weekly|monthly/date}
	suffix, err := util.ExtractTargetSuffix(targetKey)
	if checkErr(err) {
		return err
	}
	if suffix == nil {
		err := fmt.Errorf("invalid target suffix")
		checkErr(err)
		return err
	}

	targetFile := filepath.Join(targetDir, *suffix)

	exists := checkFileExists(targetFile)
	if exists {
		err := os.Remove(targetFile)
		if err != nil {
			checkErr(err)
			return err
		}
	}
	file, err := os.Create(targetFile)
	if err != nil {
		checkErr(err)
		return err
	}

	defer file.Close()

	numBytes, err := util.downloader.Download(file,
		&s3.GetObjectInput{
			Bucket: aws.String(util.Bucket),
			Key:    aws.String(targetKey),
		})
	if err != nil {
		err := fmt.Errorf("unable to download item %q, %v", targetFile, err)
		checkErr(err)
		return err
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")

	return nil
}

func (util *S3Util) ExtractTargetSuffix(targetKey string) (*string, error) {
	index := strings.Index(targetKey, "/")
	if index < 0 {
		return nil, fmt.Errorf("invalid target key %s", targetKey)
	}
	suffix := targetKey[index:]
	index = strings.Index(suffix, "/")
	if index < 0 {
		return nil, fmt.Errorf("invalid target key %s", targetKey)
	}
	suffix = suffix[index+1:]
	index = strings.Index(suffix, "/")
	if index < 0 {
		return nil, fmt.Errorf("invalid target key %s", targetKey)
	}
	suffix = suffix[index:]
	return &suffix, nil
}
