package exporter

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/jmoiron/sqlx/types"
	"github.com/pkg/errors"
)

func (m Exporter) Pages(ctx context.Context, fail func(error) error, out chan<- VFile) {
	query, err := Asset("db/query-pages.sql")
	if err != nil {
		fail(errors.Wrap(err, "Error while Retrieving Query asset"))
		return
	}
	rows, err := m.db.QueryContext(ctx, string(query))
	if err != nil {
		fail(errors.Wrap(err, "Error while Quering"))
		return
	}
	defer rows.Close()

	loadExternalData := externalDataAdapter(ctx, fail, m.extDataChannels)

	var jsonText types.JSONText
	for rows.Next() {
		if err = rows.Scan(&jsonText); err != nil {
			fail(errors.Wrap(err, "Error while Scanning"))
			return
		}
		i, err := m.jsonText2Info(jsonText)
		if err != nil {
			fail(err)
			return
		}
		loadExternalData(&i)

		templateName := "page.html"
		if i.Page.Type == _homepage {
			templateName = "homepage.html"
		}

		var b bytes.Buffer
		if err = m.templates.ExecuteTemplate(&b, templateName, i); err != nil {
			fail(errors.Wrap(err, "Error while executing template"))
			return
		}

		select {
		case <-ctx.Done():
			return
		case out <- VFile{i.FilePath(), string(b.Bytes())}:
			//Go on
		}
	}
}

func (m *Exporter) jsonText2Info(jsonText types.JSONText) (i Info, err error) {
	res := struct {
		Info
		Stats []struct {
			Indextype    string
			Measurements []YearMeasurement
		}
	}{}
	if err = jsonText.Unmarshal(&res); err != nil {
		err = errors.Wrap(err, "Error while Unmarshalling")
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

	res.Info.Exporter = m
	res.Info.ExternalFields = map[string]interface{}{}

	for i, p := range res.Info.Links {
		res.Info.Links[i].Abstract = smartTruncate(p.Abstract, 256)
	}

	return res.Info, nil
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
	*Exporter
	Page                   Page
	Index2Measurement      map[string]Measurement
	Index2YearMeasurements map[string][]YearMeasurement
	Links                  []Page
	ExternalFields         map[string]interface{}
}

func (i Info) FilePath() string {
	p := i.Page
	switch {
	case p.Type == _homepage:
		return "index.html"
	case p.Type == _topic:
		return path.Join("categories", urlsRules.Replace(Topic.UniversalFullFrom(p.ID))+".html")
	case len(p.Title) > 245: //truncate to 245 chars or less
		title := p.Title
		for index := range title { //range over runes - each may span over multiple characters
			if index < 246 {
				p.Title = title[:index]
			} else {
				break
			}
		}
		fallthrough
	default:
		return path.Join("articles", urlsRules.Replace(p.Title)+".html")
	}
}

func (i Info) CanonicalURL() string {
	URL := i.langURL
	URL.Path = path.Join(URL.Path, strings.TrimSuffix(i.FilePath(), ".html"))
	return URL.String()
}

type ranking struct {
	Rank                        int
	percentile, densePercentile float64
	Index, Among, Span          string
	Value                       float64
}

func (r ranking) Percentile() int {
	return percentage(r.percentile)
}

func (r ranking) DensePercentile() int {
	return percentage(r.densePercentile)
}

func (i Info) Rankings() (rankings []ranking) {
	for index, amm := range i.Index2Measurement {
		rankings = append(rankings, ranking{amm.Rank, amm.Percentile, amm.DensePercentile, index, "all", "all", amm.Value})
	}
	for index, ymm := range i.Index2YearMeasurements {
		for _, ym := range ymm {
			year := fmt.Sprint(ym.Year)
			rankings = append(rankings, ranking{ym.Rank, ym.Percentile, ym.DensePercentile, index, "all", year, ym.Value})
		}
	}

	if i.Page.Type != _article {
		return
	}

	for index, amm := range i.Index2Measurement {
		rankings = append(rankings, ranking{amm.TopicRank, amm.TopicPercentile, amm.TopicDensePercentile, index, i.Page.Topic(), "all", amm.Value})
	}
	for index, ymm := range i.Index2YearMeasurements {
		for _, ym := range ymm {
			year := fmt.Sprint(ym.Year)
			rankings = append(rankings, ranking{ym.TopicRank, ym.TopicPercentile, ym.TopicDensePercentile, index, i.Page.Topic(), year, ym.Value})
		}
	}
	return
}

func percentage(percentile float64) int {
	return int(percentile*100 + 0.5)
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

var urlsRules = strings.NewReplacer(" ", "_", "/", "∕", "?", "？", "#", "＃")

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
