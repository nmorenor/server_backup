package database

import (
	"fmt"
	"playus/server-backup/config"

	mars "github.com/ralfonso-directnic/mysql-backup-golang"
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
		config.Conf.Get("app.hostname").(string),
		config.Conf.Get("app.port").(string),
		config.Conf.Get("app.username").(string),
		config.Conf.Get("app.password").(string),
		config.Conf.Get("app.database").(string),    // comma separated,
		"",                                          // excluded databases
		config.Conf.Get("app.dbthreshold").(int),    //dbthreshold
		config.Conf.Get("app.tablethreshold").(int), // tablethreshold
		config.Conf.Get("app.batchsize").(int),      // batchsize
		false,                                       // forcesplit
		"",                                          // additionals
		1,                                           // verbosity
		config.Conf.Get("app.mysqldumppath").(string),
		config.Conf.Get("app.outdir").(string),
		true,
		config.Conf.Get("app.dailyrotation").(int),
		config.Conf.Get("app.weeklyrotation").(int),
		config.Conf.Get("app.monthlyrotation").(int))

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
