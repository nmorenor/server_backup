package main

import (
	"fmt"
	"sync"
	"time"

	"playus/server-backup/config"
	"playus/server-backup/database"

	"github.com/madflojo/tasks"
)

func main() {
	fmt.Println("start")
	scheduler := tasks.New()
	defer scheduler.Stop()
	// Add a task
	interval := int(config.Conf.Get("database.secondsInterval").(int))
	_, err := scheduler.Add(&tasks.Task{
		Mutex:      sync.Mutex{},
		Interval:   time.Duration(time.Duration(interval) * time.Second),
		RunOnce:    false,
		StartAfter: time.Time{},
		TaskFunc: func() error {
			fmt.Println("Start running backup: ")
			database.Worker.DoBackup()
			return nil
		},
		ErrFunc: func(err error) {
			fmt.Println("Error running backup: ")
			fmt.Println(err.Error())
		},
	})
	if err != nil {
		fmt.Println("Error scheduling backup")
	}
	database.Worker.DoBackup()
	fmt.Scanln()
	fmt.Println("end")
}
