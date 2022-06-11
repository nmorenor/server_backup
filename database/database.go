package database

import (
	"fmt"
	"playus/server-backup/config"

	mars "playus/server-backup/mars"
)

type DatabaseBackupWorker struct {
}

var (
	Worker = newDatabaseBackupWorker()
)

func newDatabaseBackupWorker() *DatabaseBackupWorker {
	worker := &DatabaseBackupWorker{}
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
		1,     // verbosity
		config.Conf.Get("database.mysqldumppath").(string),
		config.Conf.Get("database.outdir").(string),
		true,
		int(config.Conf.Get("database.dailyrotation").(int64)),
		int(config.Conf.Get("database.weeklyrotation").(int64)),
		int(config.Conf.Get("database.monthlyrotation").(int64)))

	for _, db := range options.Databases {
		mars.PrintMessage("Processing Database : "+db, options.Verbosity, mars.Info)

		tables := mars.GetTables(options.HostName, options.Bind, options.UserName, options.Password, db, options.Verbosity)
		totalRowCount := mars.GetTotalRowCount(tables)

		if !options.ForceSplit && totalRowCount <= options.DatabaseRowCountTreshold {
			// options.ForceSplit is false
			// and if total row count of a database is below defined threshold
			// then generate one file containing both schema and data

			mars.PrintMessage(fmt.Sprintf("options.ForceSplit (%t) && totalRowCount (%d) <= options.DatabaseRowCountTreshold (%d)", options.ForceSplit, totalRowCount, options.DatabaseRowCountTreshold), options.Verbosity, mars.Info)
			mars.GenerateSingleFileBackup(*options, db)
		} else if options.ForceSplit && totalRowCount <= options.DatabaseRowCountTreshold {
			// options.ForceSplit is true
			// and if total row count of a database is below defined threshold
			// then generate two files one for schema, one for data

			mars.GenerateSchemaBackup(*options, db)
			mars.GenerateSingleFileDataBackup(*options, db)
		} else if totalRowCount > options.DatabaseRowCountTreshold {
			mars.GenerateSchemaBackup(*options, db)

			for _, table := range tables {
				mars.GenerateTableBackup(*options, db, table)
			}
		}

		mars.PrintMessage("Processing done for database : "+db, options.Verbosity, mars.Info)
	}

	// Backups retentions validation
	mars.BackupRotation(*options)

}
