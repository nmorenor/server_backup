package database

import (
	"archive/tar"
	"compress/gzip"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"

	"playus/server-backup/config"
	"playus/server-backup/directory"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/fatih/color"

	_ "github.com/go-sql-driver/mysql"
)

type Options struct {
	HostName                 string
	Bind                     string
	UserName                 string
	Password                 string
	Databases                []string
	ExcludedDatabases        []string
	DatabaseRowCountTreshold int
	TableRowCountTreshold    int
	BatchSize                int
	ForceSplit               bool
	AdditionalMySQLDumpArgs  string
	Verbosity                int
	MySQLDumpPath            string
	OutputDirectory          string
	DefaultsProvidedByUser   bool
	ExecutionStartDate       time.Time
	DailyRotation            int
	WeeklyRotation           int
	MonthlyRotation          int
}

type DatabaseBackupWorker struct {
	S3Enabled bool
	Key       string
	Secret    string
	Region    string
	Endpoint  string
	S3Key     string
	Bucket    string
}

const (
	// Info messages
	Info = 1 << iota // a == 1 (iota has been reset)

	// Warning Messages
	Warning = 1 << iota // b == 2

	// Error Messages
	Error = 1 << iota // c == 4
)

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
	options := NewOptions(
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
		PrintMessage("Processing Database : "+db, options.Verbosity, Info)

		file, err := worker.GenerateSingleFileBackup(*options, db)
		if file != nil && err == nil {
			worker.upload(*file, *options)
		}

		PrintMessage("Processing done for database : "+db, options.Verbosity, Info)
	}

}

func (worker *DatabaseBackupWorker) GenerateSingleFileBackup(options Options, db string) (*string, error) {
	PrintMessage("Generating single file backup : "+db, options.Verbosity, Info)

	var args []string
	args = append(args, fmt.Sprintf("-h%s", options.HostName))
	args = append(args, fmt.Sprintf("-u%s", options.UserName))
	args = append(args, fmt.Sprintf("-p%s", options.Password))

	if options.AdditionalMySQLDumpArgs != "" {
		args = append(args, strings.Split(options.AdditionalMySQLDumpArgs, " ")...)
	}

	timestamp := strings.Replace(strings.Replace(options.ExecutionStartDate.Format("2006-01-02"), "-", "", -1), ":", "", -1)
	filename := path.Join(options.OutputDirectory, fmt.Sprintf("%s_%s.sql", db, timestamp))
	os.MkdirAll(path.Dir(filename), os.ModePerm)

	args = append(args, fmt.Sprintf("-r%s", filename))

	args = append(args, db)
	// args = append(args, "--column-statistics=0")

	PrintMessage("mysqldump is being executed with parameters : "+strings.Join(args, " "), options.Verbosity, Info)

	cmd := exec.Command(options.MySQLDumpPath, args...)
	cmdOut, _ := cmd.StdoutPipe()
	cmdErr, _ := cmd.StderrPipe()

	cmd.Start()

	output, _ := ioutil.ReadAll(cmdOut)
	err, _ := ioutil.ReadAll(cmdErr)
	cmd.Wait()

	PrintMessage("mysqldump output is : "+string(output), options.Verbosity, Info)

	if string(err) != "" && !strings.Contains(string(err), "Using a password on the command line interface can be insecure") {
		PrintMessage("mysqldump error is: "+string(err), options.Verbosity, Error)
		return nil, errors.New(string(err))
	}

	// Compressing
	PrintMessage("Compressing table file : "+filename, options.Verbosity, Info)

	// set up the output file
	file, errcreate := os.Create(filename + ".tar.gz")

	if errcreate != nil {
		PrintMessage("error to create a compressed file: "+filename, options.Verbosity, Error)
		return nil, errcreate
	}

	defer file.Close()
	// set up the gzip writer
	gw := gzip.NewWriter(file)
	defer gw.Close()
	tw := tar.NewWriter(gw)
	defer tw.Close()

	if errcompress := Compress(tw, filename); errcompress != nil {
		PrintMessage("error to compress file: "+filename, options.Verbosity, Error)
		return nil, errcompress
	}

	PrintMessage("Single file backup successfull : "+db, options.Verbosity, Info)
	return &filename, nil
}

// Compress compresses files into tar.gz file
func Compress(tw *tar.Writer, p string) error {
	file, err := os.Open(p)
	if err != nil {
		return err
	}
	defer file.Close()
	if stat, err := file.Stat(); err == nil {
		// now lets create the header as needed for this file within the tarball
		header := new(tar.Header)
		header.Name = path.Base(p)
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

func PrintMessage(message string, verbosity int, messageType int) {
	colors := map[int]color.Attribute{Info: color.FgGreen, Warning: color.FgHiYellow, Error: color.FgHiRed}

	if verbosity == 2 {
		color.Set(colors[messageType])
		fmt.Println(message)
		color.Unset()
	} else if verbosity == 1 && messageType > 1 {
		color.Set(colors[messageType])
		fmt.Println(message)
		color.Unset()
	} else if verbosity == 0 && messageType > 2 {
		color.Set(colors[messageType])
		fmt.Println(message)
		color.Unset()
	}
}

func NewOptions(hostname string, bind string, username string, password string, databases string, excludeddatabases string, databasetreshold int, tablethreshold int, batchsize int, forcesplit bool, additionals string, verbosity int, mysqldumppath string, outputDirectory string, defaultsProvidedByUser bool, dailyrotation int, weeklyrotation int, monthlyrotation int) *Options {

	databases = strings.Replace(databases, " ", "", -1)
	databases = strings.Replace(databases, " , ", ",", -1)
	databases = strings.Replace(databases, ", ", ",", -1)
	databases = strings.Replace(databases, " ,", ",", -1)
	dbs := strings.Split(databases, ",")
	dbs = removeDuplicates(dbs)

	excludeddbs := []string{}

	if databases == "--all-databases" {

		excludeddatabases = excludeddatabases + ",information_schema,performance_schema"

		dbslist := GetDatabaseList(hostname, bind, username, password, verbosity)
		databases = strings.Join(dbslist, ",")

		excludeddatabases = strings.Replace(excludeddatabases, " ", "", -1)
		excludeddatabases = strings.Replace(excludeddatabases, " , ", ",", -1)
		excludeddatabases = strings.Replace(excludeddatabases, ", ", ",", -1)
		excludeddatabases = strings.Replace(excludeddatabases, " ,", ",", -1)
		excludeddbs := strings.Split(excludeddatabases, ",")
		excludeddbs = removeDuplicates(excludeddbs)

		// Databases to not be in the backup
		dbs = difference(dbslist, excludeddbs)
	}

	return &Options{
		HostName:                 hostname,
		Bind:                     bind,
		UserName:                 username,
		Password:                 password,
		Databases:                dbs,
		ExcludedDatabases:        excludeddbs,
		DatabaseRowCountTreshold: databasetreshold,
		TableRowCountTreshold:    tablethreshold,
		BatchSize:                batchsize,
		ForceSplit:               forcesplit,
		AdditionalMySQLDumpArgs:  additionals,
		Verbosity:                verbosity,
		MySQLDumpPath:            mysqldumppath,
		OutputDirectory:          outputDirectory,
		DefaultsProvidedByUser:   defaultsProvidedByUser,
		ExecutionStartDate:       time.Now(),
		DailyRotation:            dailyrotation,
		WeeklyRotation:           weeklyrotation,
		MonthlyRotation:          monthlyrotation,
	}
}

// GetDatabaseList retrives list of databases on mysql
func GetDatabaseList(hostname string, bind string, username string, password string, verbosity int) []string {
	PrintMessage("Getting databases : "+hostname, verbosity, Info)

	db, err := sql.Open("mysql", username+":"+password+"@tcp("+hostname+":"+bind+")/mysql")
	checkErr(err)

	defer db.Close()

	rows, err := db.Query("SHOW DATABASES")
	checkErr(err)

	var result []string

	for rows.Next() {
		var databaseName string

		err = rows.Scan(&databaseName)
		checkErr(err)

		result = append(result, databaseName)
	}

	PrintMessage(strconv.Itoa(len(result))+" databases retrived : "+hostname, verbosity, Info)

	return result
}

func (worker *DatabaseBackupWorker) upload(file string, dbOptions Options) {
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

	addHandler := directory.NewAddHandler(worker.Bucket, worker.S3Key, path.Dir(file), s3Client, uploader, downloader, nil, dbOptions.DailyRotation, dbOptions.WeeklyRotation, dbOptions.MonthlyRotation)
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

func removeDuplicates(elements []string) []string {
	// Use map to record duplicates as we find them.
	encountered := map[string]bool{}
	result := []string{}

	for v := range elements {
		if encountered[elements[v]] == true {
			// Do not add duplicate.
		} else {
			// Record this element as an encountered element.
			encountered[elements[v]] = true
			// Append to result slice.
			result = append(result, elements[v])
		}
	}
	// Return the new slice.
	return result
}

// difference returns the elements in a that aren't in b
func difference(a, b []string) []string {
	mb := map[string]bool{}
	for _, x := range b {
		mb[x] = true
	}
	ab := []string{}
	for _, x := range a {
		if _, ok := mb[x]; !ok {
			ab = append(ab, x)
		}
	}
	return ab
}
