package main

import (
	"flag"
	"fmt"
	"sync"
	"time"

	"playus/server-backup/config"
	"playus/server-backup/database"
	"playus/server-backup/directory"

	"github.com/madflojo/tasks"
)

type BackupOptions struct {
	backup         bool
	restore        bool
	viewBackups    bool
	bucket         *string
	targetDir      *string
	targetKey      *string
	targetRotation *string
	targetDate     *string
}

func scheduleDBBackup(scheduler *tasks.Scheduler) {
	fmt.Println("Scheduling DB backup")
	interval := int(config.Conf.Get("database.secondsInterval").(int64))
	_, err := scheduler.Add(&tasks.Task{
		Mutex:      sync.Mutex{},
		Interval:   time.Duration(time.Duration(interval) * time.Second),
		RunOnce:    false,
		StartAfter: time.Time{},
		TaskFunc: func() error {
			fmt.Println("Start running DB backup: ")
			database.Worker.DoBackup()
			return nil
		},
		ErrFunc: func(err error) {
			fmt.Println("Error running DB backup: ")
			fmt.Println(err.Error())
		},
	})
	if err != nil {
		fmt.Println("Error scheduling DB backup")
	}
	database.Worker.DoBackup()
}

func scheduleDirBackup(scheduler *tasks.Scheduler) {
	fmt.Println("Scheduling Directory Backup")
	interval := int(config.Conf.Get("dirbackup.secondsInterval").(int64))
	_, err := scheduler.Add(&tasks.Task{
		Mutex:      sync.Mutex{},
		Interval:   time.Duration(time.Duration(interval) * time.Second),
		RunOnce:    false,
		StartAfter: time.Time{},
		TaskFunc: func() error {
			fmt.Println("Start running Dir backup: ")
			directory.Worker.DoBackup()
			return nil
		},
		ErrFunc: func(err error) {
			fmt.Println("Error running Dir backup: ")
			fmt.Println(err.Error())
		},
	})
	if err != nil {
		fmt.Println("Error scheduling Dir backup")
	}
	directory.Worker.DoBackup()
}

func runBackups() {
	fmt.Println("start")
	scheduler := tasks.New()
	defer scheduler.Stop()

	dbEnabled := config.Conf.Get("database.enabled").(bool)
	if dbEnabled {
		scheduleDBBackup(scheduler)
	}
	dirEnabled := config.Conf.Get("dirbackup.enabled").(bool)
	if dirEnabled {
		scheduleDirBackup(scheduler)
	}

	fmt.Scanln()
	fmt.Println("end")
}

func runViewBackups(bucket string) {
	worker := directory.NewBackupView(bucket)
	worker.ViewBackup()
}

func runRestore(targetDir string, bucket string, targetKey string, targetRotation string, targetDate string) {
	worker := directory.NewRestore(targetDir, bucket, fmt.Sprintf("%s/%s/%s", targetKey, targetRotation, targetDate))
	worker.RestoreBackup()
}

func main() {
	options := GetBackupOptions()
	if options == nil {
		return
	}
	if options.backup {
		runBackups()
	}
	if options.viewBackups {
		runViewBackups(*options.bucket)
	}
	if options.restore {
		runRestore(*options.targetDir, *options.bucket, *options.targetKey, *options.targetRotation, *options.targetDate)
	}
}

func GetBackupOptions() *BackupOptions {
	restore := flag.Bool("restore", false, "Indicate to run in restore mode")
	viewBackups := flag.Bool("view", false, "Get Available keys for restore")

	targetDir := flag.String("dir", "", "Target Directory to restore")
	targetBucket := flag.String("bucket", "", "Target Bucket")
	targetKey := flag.String("key", "", "Target Backup key to restore")
	targetRotation := flag.String("rotation", "", "Target Rotation key, daily|weekly|monthly")
	targetDate := flag.String("date", "", "Target date directory to restore")

	flag.Parse()
	if (restore == nil && viewBackups == nil) || !(*viewBackups) && !(*restore) {
		return &BackupOptions{
			backup:         true,
			restore:        false,
			viewBackups:    false,
			targetDir:      nil,
			targetKey:      nil,
			targetRotation: nil,
			targetDate:     nil,
		}
	}
	if restore != nil && (*restore) {
		if targetDir == nil || *targetDir == "" || targetKey == nil || *targetKey == "" || targetRotation == nil || *targetRotation == "" || targetDate == nil || *targetDate == "" || targetBucket == nil || *targetBucket == "" {
			flag.Usage()
			return nil
		}
		return &BackupOptions{
			backup:         false,
			restore:        true,
			viewBackups:    false,
			targetDir:      targetDir,
			targetKey:      targetKey,
			targetRotation: targetRotation,
			targetDate:     targetDate,
			bucket:         targetBucket,
		}
	}
	if viewBackups != nil && (*viewBackups) {
		if targetBucket == nil || *targetBucket == "" {
			fmt.Println("To run view backups the target bucket is required")
			flag.Usage()
			return nil
		}
		return &BackupOptions{
			backup:         false,
			restore:        false,
			viewBackups:    true,
			targetDir:      nil,
			targetKey:      nil,
			targetRotation: nil,
			targetDate:     nil,
			bucket:         targetBucket,
		}
	}
	return nil
}
