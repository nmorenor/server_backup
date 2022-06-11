package main

import (
	"fmt"
	"sync"
	"time"

	"playus/server-backup/database"

	"github.com/madflojo/tasks"
)

func main() {
	fmt.Println("start")
	scheduler := tasks.New()
	defer scheduler.Stop()
	// Add a task
	_, err := scheduler.Add(&tasks.Task{
		Mutex:      sync.Mutex{},
		Interval:   time.Duration(24 * time.Hour),
		RunOnce:    false,
		StartAfter: time.Time{},
		TaskFunc: func() error {
			database.Worker.DoBackup()
			return nil
		},
		ErrFunc: func(error) {
		},
	})
	if err != nil {
		fmt.Println("Error scheduling backup")
	}
	database.Worker.DoBackup()
	fmt.Scanln()
	fmt.Println("end")
}
