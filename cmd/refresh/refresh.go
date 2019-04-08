package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/ebonetti/overpedia/internal/exporter"
	"github.com/ebonetti/overpedia/internal/preprocessor"
	"github.com/ebonetti/wikiassignment/nationalization"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

var lang, dataSource, dbopts, indices string
var keepSavepoints bool
var filterBots, test bool

func init() {
	flag.StringVar(&lang, "lang", "it", "Wikipedia nationalization to parse.")
	flag.StringVar(&dataSource, "source", "net", "Source of data (net,csv,db).")
	flag.StringVar(&dbopts, "db", "user=postgres dbname=postgres sslmode=disable", "Options for connecting to the db.")
	flag.StringVar(&indices, "indices", "default", "Indices to use in graphs (default,alternate).")
	flag.BoolVar(&keepSavepoints, "keep", false, "Keep every savepoint - csv and db - after the execution (true or false).")
	flag.BoolVar(&filterBots, "nobots", false, "Filter every edit done by a Bot before CSV exporting.")
	flag.BoolVar(&test, "test", false, "Run as test on a fraction of the articles before CSV exporting.")
}

func main() {
	stackTraceOn(syscall.SIGUSR1) //enable logging to a file the current stack trace upon receiving the signal SIGUSR1
	flag.Parse()
	log.Println("Called with the command: ", strings.Join(os.Args, " "))
	log.Printf("Interpreted as: refresh -lang = %s -source = %s -db = '%s' -indices = %s -keep = %t -nobots = %t -test = %t\n", lang, dataSource, dbopts, indices, keepSavepoints, filterBots, test)

	start := time.Now()
	defer func() {
		log.Println("Time elapsed since start: ", time.Since(start))
	}()

	var tarball *tar.Writer
	{
		f, err := os.Create("overpedia.tar.gz")
		if err != nil {
			log.Panic(err)
		}
		defer f.Close()

		b := bufio.NewWriter(f)
		defer b.Flush()

		g := gzip.NewWriter(b)
		defer g.Close()

		tarball = tar.NewWriter(g)
		defer tarball.Close()
	}

	_, err := nationalization.New(lang)
	if err != nil {
		log.Panicf("%+v", err)
	}

	db, err := getDB()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	const csvDir = "csv"
	err = os.MkdirAll(csvDir, 777)
	if err != nil {
		log.Panicf("%+v", err)
	}

	m, dbDestructor, err := exporter.OpenModel(db, lang)
	switch dataSource {
	case "net":
		log.Print("Started data preprocessing and CSV export")
		err = preprocessor.Run(context.Background(), csvDir, lang, filterBots, test)
		if err != nil {
			break
		}
		fallthrough
	case "csv":
		log.Print("Started CSV importing into DB")
		m, dbDestructor, err = exporter.NewModel(context.Background(), db, indices, lang, csvDir)
	case "db":
		//Do nothing, already opened
	default:
		err = errors.New("error: datasource " + dataSource + " not supported")
	}
	if err != nil {
		log.Fatalf("%+v", err)
	}

	if !keepSavepoints {
		defer func() {
			os.RemoveAll(csvDir)
			dbDestructor()
		}()
	}

	log.Print("Started tarball dump")
	var b bytes.Buffer
	err = exporter.Walk(context.Background(), m, func(path string, info os.FileInfo, e error) (err error) {
		if e != nil {
			return e
		}

		if info.IsDir() {
			return
		}

		b.Reset()
		b.Write(info.(interface {
			Data() []byte
		}).Data())
		compressor := gzip.NewWriter(&b)
		if _, err = io.CopyN(compressor, &b, int64(b.Len())); err != nil {
			return
		}
		if err = compressor.Close(); err != nil {
			return
		}

		header, err := tar.FileInfoHeader(newVFile(path+".gz", b.Bytes()), "")
		if err != nil {
			return
		}
		if err = tarball.WriteHeader(header); err != nil {
			return
		}
		_, err = tarball.Write(b.Bytes())
		return
	})
	if err != nil {
		log.Fatalf("%+v", err)
	}
	log.Print("Tarball dump exported successfully")
}

func getDB() (db *sqlx.DB, err error) {
	for t := time.Second; t < 5*time.Minute; t *= 2 { //exponential backoff
		db, err = sqlx.Connect("postgres", dbopts)
		if err == nil {
			log.Print("Connected to the database")
			return
		}
		err = errors.Wrap(err, "Unable to connect to the database")
		if t > 30*time.Second {
			log.Print(err.Error())
		}
		time.Sleep(t)
	}
	return
}

func stackTraceOn(sig ...os.Signal) {
	sigChan := make(chan os.Signal)
	go func() {
		stacktrace := make([]byte, 8388608)
		for range sigChan {
			stacktrace := stacktrace[:runtime.Stack(stacktrace, true)]
			filename := fmt.Sprint("stacktrace ", time.Now().Format("2006-01-02 15:04:05"))
			err := ioutil.WriteFile(filename, stacktrace, os.ModePerm)
			if err != nil {
				log.Print("While writing stack trace on file encountered the following error ", err)
				log.Print("Stack trace follows: ", string(stacktrace))
			}
		}
	}()
	signal.Notify(sigChan, sig...)
}
