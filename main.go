package main

import (
	"fmt"
	"sync"
	"time"

	"playus/server-backup/config"
	"playus/server-backup/database"
	"playus/server-backup/directory"

	"github.com/madflojo/tasks"
)

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
}

func main() {
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
