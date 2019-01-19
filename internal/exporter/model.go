package exporter

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	_ "github.com/lib/pq" //postgresql driver
	"github.com/pkg/errors"
)

type mData struct {
	Lang          string
	BoundingYears struct {
		Min, Max                   int
		MinTimestamp, MaxTimestamp time.Time
	}
}

type Model struct {
	db   *sqlx.DB
	data mData
}

func NewModel(ctx context.Context, db *sqlx.DB, indices, lang, sourcePath string) (m Model, destructor func(), err error) {
	sourcePath, err = filepath.Abs(sourcePath)
	if err != nil {
		err = errors.Wrap(err, "Error while converting source path to absolute")
		return
	}
	fail := func(e error) (Model, func(), error) {
		getDestructor(db)()
		m, destructor, err = Model{}, nil, e
		return m, destructor, err
	}

	query := ""
	for _, dbfile := range []string{"db/base.sql", "db/indices/" + indices + ".sql", "db/types.sql"} {
		var b []byte
		if b, err = Asset(dbfile); err != nil {
			return fail(errors.Wrap(err, err.Error()+" while opening "+dbfile))
		}
		query += string(b)
	}
	for _, name := range []string{"pages", "revisions", "socialjumps"} {
		query = strings.Replace(query, ":'"+name+"filepath'", "'"+filepath.Join(sourcePath, name)+".csv'", -1)
	}

	for _, query := range strings.Split(query, ";") {
		if _, err = db.ExecContext(ctx, query); err != nil {
			return fail(errors.Wrap(err, err.Error()+" while executing the following query:\n"+query))
		}
	}

	m, destructor, err = OpenModel(db, lang)
	if err != nil {
		return fail(err)
	}

	return
}

func OpenModel(db *sqlx.DB, lang string) (m Model, destructor func(), err error) {
	fail := func(e error) (Model, func(), error) {
		m, destructor, err = Model{}, nil, e
		return m, destructor, err
	}

	m.db = db
	m.data.Lang = lang

	err = db.Get(&m.data.BoundingYears, "SELECT minyear AS Min, maxyear AS Max, mintimestamp AS MinTimestamp, maxtimestamp AS MaxTimestamp FROM w2o.timebounds;")
	if err != nil {
		return fail(errors.Wrap(err, "Error while retrieving Timebounds"))
	}

	destructor = getDestructor(db)

	return
}

func getDestructor(db *sqlx.DB) func() {
	return func() {
		db.Exec("DROP SCHEMA IF EXISTS w2o CASCADE;")
	}
}

func (m Model) Data() mData {
	return mData{m.data.Lang, m.data.BoundingYears}
}

func (m Model) Lang() string {
	return m.data.Lang
}

func (m Model) BoundingYears() (minYear, maxYear int) {
	return m.data.BoundingYears.Min, m.data.BoundingYears.Max
}

type Page struct {
	ID              uint32
	Title, Abstract string
	ParentID        uint32
	Type            string
	CreationYear    int
}

const (
	_article  = "article"
	_topic    = "topic"
	_homepage = "global"
)

func (p Page) Topic() string {
	switch p.Type {
	case _article:
		return Topic.UniversalFrom(p.ParentID)
	case _topic:
		return Topic.UniversalFrom(p.ID)
	default: //homepage
		return ""
	}
}

func (p Page) UnderscoredTitle() string {
	return strings.Replace(p.Title, " ", "_", -1)
}

type Measurement struct {
	Value, Percentile, DensePercentile    float64
	Rank                                  int
	TopicPercentile, TopicDensePercentile float64
	TopicRank                             int
}

type YearMeasurement struct {
	Measurement
	Year int
}

type Info struct {
	Page                   Page
	Index2Measurement      map[string]Measurement
	Index2YearMeasurements map[string][]YearMeasurement
	Links                  []Page
}

func (m Model) Pages(ctx context.Context, process func(Info) error) (err error) {
	query, err := Asset("db/query-pages.sql")
	if err != nil {
		return
	}
	rows, err := m.db.QueryContext(ctx, string(query))
	if err != nil {
		return
	}
	defer rows.Close()

	var jsonText types.JSONText
	for rows.Next() {
		if err = rows.Scan(&jsonText); err != nil {
			return
		}

		res := struct {
			Info
			Stats []struct {
				Indextype    string
				Measurements []YearMeasurement
			}
		}{}
		if err = jsonText.Unmarshal(&res); err != nil {
			return
		}

		index2Measurement := make(map[string]Measurement, len(res.Stats))
		index2YearMeasurements := make(map[string][]YearMeasurement, len(res.Stats))
		for _, e := range res.Stats {
			index2Measurement[e.Indextype] = e.Measurements[0].Measurement
			index2YearMeasurements[e.Indextype] = e.Measurements[1:]
		}
		res.Index2Measurement = index2Measurement
		res.Index2YearMeasurements = index2YearMeasurements

		if err = process(res.Info); err != nil {
			return
		}
	}
	return
}

type AnnualIndexesRanking struct {
	Year          uint32
	Index2Ranking map[string][]Page
}

func (m Model) TopTen(ctx context.Context, process func(AnnualIndexesRanking) error) (err error) {
	query, err := Asset("db/query-toptenbyyear.sql")
	if err != nil {
		return err
	}
	rows, err := m.db.QueryContext(ctx, string(query))
	if err != nil {
		return err
	}
	defer rows.Close()

	var jsonText types.JSONText
	for rows.Next() {
		if err = rows.Scan(&jsonText); err != nil {
			return err
		}
		res := struct {
			Year           uint32
			Indexesranking []struct {
				Index   string
				Ranking []Page
			}
		}{}
		if err = jsonText.Unmarshal(&res); err != nil {
			return
		}

		index2Ranking := make(map[string][]Page, len(res.Indexesranking))
		for _, e := range res.Indexesranking {
			index2Ranking[e.Index] = e.Ranking
		}

		if err = process(AnnualIndexesRanking{res.Year, index2Ranking}); err != nil {
			return
		}
	}
	return
}
