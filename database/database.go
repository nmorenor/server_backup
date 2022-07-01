package database

import (
	"fmt"
	"path"

	"playus/server-backup/config"
	"playus/server-backup/directory"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	mars "playus/server-backup/mars"
)

type DatabaseBackupWorker struct {
	S3Enabled bool
	Key       string
	Secret    string
	Region    string
	Endpoint  string
	S3Key     string
	Bucket    string
}

var (
	Worker = newDatabaseBackupWorker()
)

func newDatabaseBackupWorker() *DatabaseBackupWorker {
	worker := &DatabaseBackupWorker{
		S3Enabled: config.Conf.Get("database.s3Backup").(bool),
		Key:       config.Conf.Get("database.key").(string),
		Secret:    config.Conf.Get("database.secret").(string),
		Region:    config.Conf.Get("database.region").(string),
		Endpoint:  config.Conf.Get("database.endpoint").(string),
		S3Key:     config.Conf.Get("database.s3Key").(string),
		Bucket:    config.Conf.Get("database.bucket").(string),
	}
	return worker
}

func (worker *DatabaseBackupWorker) DoBackup() {
	options := mars.NewOptions(
		config.Conf.Get("database.hostname").(string),
		config.Conf.Get("database.port").(string),
		config.Conf.Get("database.username").(string),
		config.Conf.Get("database.password").(string),
		config.Conf.Get("database.database").(string), // comma separated,
		"", // excluded databases
		int(config.Conf.Get("database.dbthreshold").(int64)),    //dbthreshold
		int(config.Conf.Get("database.tablethreshold").(int64)), // tablethreshold
		int(config.Conf.Get("database.batchsize").(int64)),      // batchsize
		false, // forcesplit
		"",    // additionals
		int(config.Conf.Get("database.verbosity").(int64)), // verbosity
		config.Conf.Get("database.mysqldumppath").(string),
		config.Conf.Get("database.outdir").(string),
		true,
		int(config.Conf.Get("database.dailyrotation").(int64)),
		int(config.Conf.Get("database.weeklyrotation").(int64)),
		int(config.Conf.Get("database.monthlyrotation").(int64)))

	for _, db := range options.Databases {
		mars.PrintMessage("Processing Database : "+db, options.Verbosity, mars.Info)

		file, err := mars.GenerateSingleFileBackup(*options, db)
		if file != nil && err == nil {
			worker.upload(*file, *options)
		}

		mars.PrintMessage("Processing done for database : "+db, options.Verbosity, mars.Info)
	}

	// Backups retentions validation
	mars.BackupRotation(*options)

}

func (worker *DatabaseBackupWorker) upload(file string, dbOptions mars.Options) {
	if !worker.S3Enabled {
		return
	}
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

	addHandler := directory.NewAddHandler(worker.Bucket, worker.S3Key, path.Dir(file), s3Client, uploader, downloader, dbOptions.DailyRotation, dbOptions.WeeklyRotation, dbOptions.MonthlyRotation)
	addHandler.Handle()

	removeHandler := directory.NewRemoveHandler(worker.Bucket, worker.S3Key, path.Dir(file), s3Client, uploader, downloader, dbOptions.DailyRotation, dbOptions.WeeklyRotation, dbOptions.MonthlyRotation)
	removeHandler.Handle()
}

func checkErr(err error) bool {
	if err != nil {
		fmt.Println(fmt.Printf("[ERROR] %s", err))
		return true
	}
	return false
}
