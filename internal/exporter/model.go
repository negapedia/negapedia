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
	Topics        []Page
	Indexes       []string
	DefaultIndex  string
	BoundingYears struct {
		Min, Max                   int
		MinTimestamp, MaxTimestamp time.Time
	}
	PageDepth2Count []int
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

	err = db.Select(&m.data.Topics, "SELECT page_title AS title, page_abstract AS abstract, topic_title AS topic, istopic AS istopic FROM w2o.topicpages WHERE page_depth = 1 ORDER BY page_title;")
	if err != nil {
		return fail(errors.Wrap(err, "Error while retrieving Topics"))
	}
	err = db.Select(&m.data.Indexes, "SELECT type::name as name FROM unnest(enum_range(NULL::w2o.myindex)) AS _(type) ORDER BY name;")
	if err != nil {
		return fail(errors.Wrap(err, "Error while retrieving Indexes"))
	}
	if len(m.data.Indexes) == 0 {
		return fail(errors.New("Error: there are no indexes defined over data"))
	}
	m.data.DefaultIndex = m.data.Indexes[0]

	err = db.Get(&m.data.BoundingYears, "SELECT minyear AS Min, maxyear AS Max, mintimestamp AS MinTimestamp, maxtimestamp AS MaxTimestamp FROM w2o.timebounds;")
	if err != nil {
		return fail(errors.Wrap(err, "Error while retrieving Timebounds"))
	}
	err = db.Select(&m.data.PageDepth2Count, "SELECT count FROM (SELECT page_depth,COUNT(*) as count FROM w2o.pages GROUP BY page_depth) AS _ ORDER BY page_depth;")
	if err != nil {
		return fail(errors.Wrap(err, "Error while retrieving PageDepth2Count"))
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
	return mData{m.data.Lang, m.Topics(), m.Indexes(), m.DefaultIndex(), m.data.BoundingYears, m.PageDepth2Count()}
}

func (m Model) Lang() string {
	return m.data.Lang
}

func (m Model) Topics() []Page {
	return append([]Page{}, m.data.Topics...)
}

func (m Model) Indexes() []string {
	return append([]string{}, m.data.Indexes...)
}

func (m Model) DefaultIndex() string {
	return m.data.DefaultIndex
}

func (m Model) BoundingYears() (minYear, maxYear int) {
	return m.data.BoundingYears.Min, m.data.BoundingYears.Max
}

func (m Model) PageDepth2Count() []int {
	return append([]int{}, m.data.PageDepth2Count...)
}

type Page struct {
	Title, Abstract, Topic string
	IsTopic                bool
}

func (p Page) UnderscoredTitle() string {
	return strings.Replace(p.Title, " ", "_", -1)
}

type ExtendedPage struct {
	Page
	FullTopic    string
	CreationYear int
	PageDepth    int
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
	Page                   ExtendedPage
	Index2Measurement      map[string]Measurement
	Index2YearMeasurements map[string][]YearMeasurement
	Links, HotArticles     []Page
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
	Year          int
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
			Year           int
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
