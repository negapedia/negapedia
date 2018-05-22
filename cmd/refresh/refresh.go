package main

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ebonetti/overpedia/internal/exporter"
	"github.com/ebonetti/overpedia/internal/preprocessor"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

//go:generate go-bindata -pkg $GOPACKAGE -prefix "nationalizations/" nationalizations/...

var lang, dataSource, dbopts string
var keepSavepoints bool
var filterBots bool

func init() {
	flag.StringVar(&lang, "lang", "it", "Wikipedia nationalization to parse (en,it).")
	flag.StringVar(&dataSource, "source", "net", "Source of data (net,csv,db).")
	flag.StringVar(&dbopts, "db", "user=postgres dbname=postgres sslmode=disable", "Options for connecting to the db.")
	flag.BoolVar(&keepSavepoints, "keep", false, "Keep every savepoint - csv and db - after the execution (true or false).")
	flag.BoolVar(&filterBots, "nobots", false, "Filter every edit done by a Bot before CSV exporting.")
}

func main() {
	flag.Parse()
	log.Println("Called with the command: ", strings.Join(os.Args, " "))
	log.Printf("Interpreted as: refresh lang = %s source = %s db = '%s' keep = %t nobots = %t\n", lang, dataSource, dbopts, keepSavepoints, filterBots)

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

	nazionalization, err := nationalization(lang)
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
		err = preprocessor.Run(context.Background(), csvDir, filterBots, nazionalization)
		if err != nil {
			break
		}
		fallthrough
	case "csv":
		log.Print("Started CSV importing into DB")
		m, dbDestructor, err = exporter.NewModel(context.Background(), db, lang, csvDir)
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

func nationalization(lang string) (data preprocessor.Nationalization, err error) {
	bytes, err := Asset(lang + ".json")
	if err != nil {
		return
	}

	data.Article2Topic = map[uint32]uint32{}

	if err = json.Unmarshal(bytes, &data); err != nil {
		data = preprocessor.Nationalization{}
	}

	return
}
