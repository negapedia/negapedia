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
	"net/url"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/ebonetti/ctxutils"

	"github.com/jmoiron/sqlx"
	"github.com/negapedia/negapedia/internal/exporter"
	"github.com/negapedia/negapedia/internal/preprocessor"
	"github.com/negapedia/wikiassignment/nationalization"
	"github.com/negapedia/wikibrief"
	"github.com/negapedia/wikitfidf"
	"github.com/pkg/errors"
)

var lang, dataSource, baseURL, dbopts string
var keepSavepoints bool
var noTFIDF, test bool

func init() {
	flag.StringVar(&lang, "lang", "it", "Wikipedia nationalization to parse.")
	flag.StringVar(&dataSource, "source", "net", "Source of data (net,csv).")
	flag.StringVar(&baseURL, "URL", "http://%s.negapedia.org", "Output base URL, '%s' is the optional placeholder for subdomain.")
	flag.StringVar(&dbopts, "db", "user=postgres dbname=postgres sslmode=disable", "Options for connecting to the db.")
	flag.BoolVar(&keepSavepoints, "keep", false, "Keep every savepoint after the execution (true or false).")
	flag.BoolVar(&noTFIDF, "notfidf", false, "do not calculate TFID, if avaible use precalculated measures.")
	flag.BoolVar(&test, "test", false, "Run as test on a fraction of the articles before CSV exporting.")
}

func main() {
	stackTraceOn(syscall.SIGUSR1) //enable logging to a file the current stack trace upon receiving the signal SIGUSR1
	flag.Parse()
	log.Println("Called with the command: ", strings.Join(os.Args, " "))
	log.Printf("Interpreted as: refresh -lang = %s -URL = %s -source = %s -db = '%s' -keep = %t -notfidf = %t -test = %t\n", lang, baseURL, dataSource, dbopts, keepSavepoints, noTFIDF, test)

	start := time.Now()
	defer func() {
		log.Println("Time elapsed since start: ", time.Since(start))
	}()

	ctx, fail := ctxutils.WithFail(context.Background())

	_, err := nationalization.New(lang)
	if err != nil {
		log.Fatalf("%+v", fail(err))
	}

	const csvDir = "csv"
	err = os.MkdirAll(csvDir, 777)
	if err != nil {
		log.Fatalf("%+v", fail(err))
	}

	if dataSource == "net" {
		log.Print("Started data preprocessing and CSV export")
		preprocess(ctx, fail, csvDir, lang, test)
		if ctx.Err() != nil {
			log.Fatalf("%+v", fail(nil))
		}
	} else if dataSource != "csv" {
		log.Fatalf("%+v", fail(errors.New("error: datasource "+dataSource+" not supported")))
	}

	log.Print("Started CSV importing into DB")
	db, err := getDB()
	if err != nil {
		log.Fatalf("%+v", fail(err))
	}

	wwwURL, langURL, err := getURLs()
	if err != nil {
		log.Fatalf("%+v", fail(err))
	}

	m, dbDestructor, err := exporter.From(ctx, db, lang, csvDir, wwwURL, langURL, TFIDFExporter(ctx, fail, tfidf)...)
	if err != nil {
		log.Fatalf("%+v", fail(err))
	}

	if ctx.Err() != nil {
		log.Fatalf("%+v", fail(nil))
	}

	if !keepSavepoints {
		defer func() {
			os.RemoveAll(csvDir)
			tfidf.Delete()
			dbDestructor()
		}()
	}

	var tarball *tar.Writer
	{
		f, err := os.Create("overpedia.tar.gz")
		if err != nil {
			log.Panic(err)
		}
		defer f.Close()

		b := bufio.NewWriter(f)
		defer b.Flush()

		g, _ := gzip.NewWriterLevel(b, gzip.BestCompression)
		defer g.Close()

		tarball = tar.NewWriter(g)
		defer tarball.Close()
	}

	if tfidf.Lang != "" { //TFIDF data is optional
		log.Print("Started tarball dump")
	} else {
		log.Print("Started tarball dump (without TFIDF data)")
	}
	var b bytes.Buffer

	for vfile := range m.Everything(ctx, fail) {
		b.Reset()
		b.Write([]byte(vfile.Data))
		compressor, _ := gzip.NewWriterLevel(&b, gzip.BestCompression)
		if _, err = io.CopyN(compressor, &b, int64(b.Len())); err != nil {
			fail(err)
			break
		}
		if err = compressor.Close(); err != nil {
			fail(err)
			break
		}

		header, err := tar.FileInfoHeader(newVFile(path.Join("html", vfile.Path+".gz"), b.Bytes()), "")
		if err != nil {
			fail(err)
			break
		}
		if err = tarball.WriteHeader(header); err != nil {
			fail(err)
			break
		}
		_, err = tarball.Write(b.Bytes())
		if err != nil {
			fail(err)
			break
		}
	}

	if err = fail(nil); err != nil {
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

func getURLs() (wwwURL, langURL url.URL, err error) {
	switch strings.Count(baseURL, "%s") {
	case 0:
		baseURL += "%.0s"
	case 1:
		//Nothing to do
	default:
		err = errors.New("Invalid URL: too many %s formatting placeholders in " + baseURL)
		return
	}

	wwwURLp, err := url.Parse(fmt.Sprintf(baseURL, "www"))
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	langURLp, err := url.Parse(fmt.Sprintf(baseURL, lang))
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	return *wwwURLp, *langURLp, nil
}

var tfidf wikitfidf.Exporter

func init() {
	//Default initialization from pre-calculated data if existent
	tfidf, _ = wikitfidf.From(lang, "TFIDF")
}

func preprocess(ctx context.Context, fail func(error) error, CSVDir, lang string, test bool) {
	process := []preprocessor.Process{}
	if !noTFIDF && wikitfidf.CheckAvailableLanguage(lang) == nil {
		process = append(process, func(ctx context.Context, fail func(error) error, articles <-chan wikibrief.EvolvingPage) {
			var tfidfErr error
			tfidf, tfidfErr = wikitfidf.New(ctx, lang, articles, ".", wikitfidf.ReasonableLimits(), test)
			if tfidfErr != nil {
				fail(tfidfErr)
			}
		})
	}
	if err := preprocessor.Run(ctx, CSVDir, lang, test, process...); err != nil {
		fail(err)
	}
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
