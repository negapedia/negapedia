package exporter

import (
	"context"
	"html/template"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq" //postgresql driver
	"github.com/pkg/errors"
)

//Download database structure
//go:generate rm -fr wiki2overpediadb
//go:generate git clone https://github.com/negapedia/wiki2overpediadb.git
//go:generate mv wiki2overpediadb/db/ db/

//Regenerate bindata
//go:generate go-bindata -pkg $GOPACKAGE db/... templates/...

//Delete everything created or downloaded
//go:generate rm -fr wiki2overpediadb db

func From(ctx context.Context, db *sqlx.DB, lang, csvPath string, wwwURL, langURL url.URL, extDataChannels ...<-chan ExtData) (m Exporter, destructor func(), err error) {
	csvPath, err = filepath.Abs(csvPath)
	if err != nil {
		err = errors.Wrap(err, "Error while converting source path to absolute")
		return
	}
	fail := func(e error) (Exporter, func(), error) {
		getDestructor(db)()
		m, destructor, err = Exporter{}, nil, e
		return m, destructor, err
	}

	query := ""
	for _, dbfile := range []string{"db/base.sql", "db/indices.sql", "db/types.sql"} {
		var b []byte
		if b, err = Asset(dbfile); err != nil {
			return fail(errors.Wrap(err, err.Error()+" while opening "+dbfile))
		}
		query += string(b)
	}
	for _, name := range []string{"pages", "revisions", "socialjumps"} {
		query = strings.Replace(query, ":'"+name+"filepath'", "'"+filepath.Join(csvPath, name)+".csv'", -1)
	}

	for _, query := range strings.Split(query, ";") {
		if _, err = db.ExecContext(ctx, query); err != nil {
			return fail(errors.Wrap(err, err.Error()+" while executing the following query:\n"+query))
		}
	}

	m, destructor, err = Open(ctx, db, lang, wwwURL, langURL, extDataChannels...)
	if err != nil {
		return fail(err)
	}

	return
}

func Open(ctx context.Context, db *sqlx.DB, lang string, wwwURL, langURL url.URL, extDataChannels ...<-chan ExtData) (m Exporter, destructor func(), err error) {
	fail := func(e error) (Exporter, func(), error) {
		m, destructor, err = Exporter{}, nil, e
		return m, destructor, err
	}

	m.db = db
	m.lang = lang
	m.wwwURL, m.langURL = wwwURL, langURL
	m.extDataChannels = extDataChannels

	err = db.GetContext(ctx, &m.boundingYears, "SELECT minyear AS Min, maxyear AS Max, mintimestamp AS MinTimestamp, maxtimestamp AS MaxTimestamp FROM w2o.timebounds;")
	if err != nil {
		return fail(errors.Wrap(err, "Error while retrieving Timebounds"))
	}

	destructor = getDestructor(db)

	m.templates, err = templates(langURL)
	if err != nil {
		return fail(errors.Wrap(err, "Error while retrieving Templates"))
	}

	return
}

type Exporter struct {
	db              *sqlx.DB
	lang            string
	wwwURL, langURL url.URL
	boundingYears   struct {
		Min, Max                   int64
		MinTimestamp, MaxTimestamp time.Time
	}
	templates       *template.Template
	extDataChannels []<-chan ExtData
}

func getDestructor(db *sqlx.DB) func() {
	return func() {
		db.Exec("DROP SCHEMA IF EXISTS w2o CASCADE;")
	}
}

func (m Exporter) Everything(ctx context.Context, fail func(error) error) <-chan VFile {
	out := make(chan VFile, 1000)
	go func() {
		defer close(out)
		type FExporter func(context.Context, func(error) error, chan<- VFile)
		var wg sync.WaitGroup
		for _, f := range []FExporter{m.Pages, m.TopTens} {
			wg.Add(1)
			go func(f FExporter) {
				defer wg.Done()
				f(ctx, fail, out)
			}(f)
		}
		wg.Wait()
	}()

	return out
}

type VFile struct {
	Path, Data string
}

func (m Exporter) Lang() string {
	return m.lang
}

func (m Exporter) WWWURL() string {
	return m.wwwURL.String()
}

func (m Exporter) MinYear() int64 {
	return m.boundingYears.Min
}

func (m Exporter) MaxYear() int64 {
	return m.boundingYears.Max
}

func (m Exporter) MinTimestamp() int64 {
	return m.boundingYears.MinTimestamp.Unix()
}

func (m Exporter) MaxTimestamp() int64 {
	return m.boundingYears.MaxTimestamp.Unix()
}
