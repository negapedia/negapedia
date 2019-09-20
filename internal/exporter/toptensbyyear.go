package exporter

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/jmoiron/sqlx/types"
	"github.com/pkg/errors"
)

func (m Exporter) TopTens(ctx context.Context, fail func(error) error, out chan<- VFile) {
	query, err := Asset("db/query-toptenbyyear.sql")
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

	var jsonText types.JSONText
	for rows.Next() {
		if err = rows.Scan(&jsonText); err != nil {
			fail(errors.Wrap(err, "Error while Scanning"))
			return
		}

		toptensInfo, err := m.jsonText2TopTens(jsonText)
		if err != nil {
			fail(err)
			return
		}

		for _, topten := range toptensInfo {
			var b bytes.Buffer
			if err = m.templates.ExecuteTemplate(&b, "topten.html", topten); err != nil {
				fail(errors.Wrap(err, "Error while executing template"))
				return
			}

			select {
			case <-ctx.Done():
				return
			case out <- VFile{topten.FilePath(), string(b.Bytes())}:
				//Go on
			}
		}
	}
}

func (m *Exporter) jsonText2TopTens(jsonText types.JSONText) (ii []TopTenInfo, err error) {
	res := rawTopten{}
	if err = jsonText.Unmarshal(&res); err != nil {
		return
	}

	for i, index := range res.IndexesRanking {
		for j, p := range index.Ranking {
			res.IndexesRanking[i].Ranking[j].Abstract = smartTruncate(p.Abstract, 512)
		}
	}

	for _, t := range splitRawTopten(res) {
		ii = append(ii, TopTenInfo{m, t})
	}

	return
}

type rawTopten struct {
	Year           uint32
	IndexesRanking []struct {
		Index   string
		Ranking []Page
	}
}

type TopTenInfo struct {
	*Exporter
	SplittedAnnualIndexRanking
}

type SplittedAnnualIndexRanking struct {
	TTKey
	Ranking []Page
}

type TTKey struct {
	Year    uint32 //0 iff it's all time
	Index   string
	TopicID uint32 //0 iff it's all
}

func (i TopTenInfo) FilePath() string {
	year := fmt.Sprint(i.Year)
	if i.Year == 0 {
		year = "all"
	}
	topic := Topic.UniversalFrom(i.TopicID)
	if i.TopicID == 0 {
		topic = "all"
	}

	return path.Join("toptens", year, i.Index, topic+".html")
}

func (i TopTenInfo) CanonicalURL() string {
	URL := i.langURL
	URL.Path = path.Join(URL.Path, strings.TrimSuffix(i.FilePath(), ".html"))
	return URL.String()
}

func (i TopTenInfo) Span() string {
	span := fmt.Sprint(i.Year)
	if i.Year == 0 {
		span = "all"
	}
	return span
}

func (i TopTenInfo) Title() string {
	title := i.Span()
	if i.Year == 0 {
		title = "All Time"
	}

	title += " Top Ten of " + strings.Title(i.Index)

	topic := i.Topic()
	if topic != "all" {
		title += " for " + strings.Title(topic)
	}

	return title
}

func (i TopTenInfo) Topic() string {
	topic := "all"
	if i.TopicID != 0 {
		topic = Topic.UniversalFrom(i.TopicID)
	}
	return topic
}

func splitRawTopten(rawTopten rawTopten) (rankings []SplittedAnnualIndexRanking) {
	years, indexes, topics := map[uint32]bool{}, map[string]bool{}, map[uint32]bool{}
	push := func(r SplittedAnnualIndexRanking) {
		years[r.Year], indexes[r.Index], topics[r.TopicID] = true, true, true
		p := sort.Search(len(rankings), func(i int) (ismoreorequal bool) {
			ri := rankings[i]
			switch {
			case ri.Year-1 < r.Year-1: //trick to consider 0 as maximum
				return false
			case ri.Year-1 > r.Year-1: //trick to consider 0 as maximum
				return true
			case ri.Index < r.Index:
				return false
			case ri.Index > r.Index:
				return true
			case ri.TopicID < r.TopicID:
				return false
			default: //case ri.TopicID >= r.TopicID:
				return true
			}
		})
		if p < len(rankings) && r.TTKey == rankings[p].TTKey {
			rankings[p].Ranking = append(rankings[p].Ranking, r.Ranking...)
			return
		}
		rks := append(rankings, SplittedAnnualIndexRanking{})
		copy(rks[p+1:], rks[p:])
		rks[p] = r
		rankings = rks
	}

	//Actually use the data
	for _, indexRanking := range rawTopten.IndexesRanking {
		index := indexRanking.Index
		pp := indexRanking.Ranking
		for _, p := range pp {
			push(SplittedAnnualIndexRanking{TTKey{rawTopten.Year, index, p.ParentID}, []Page{p}})
		}
		if len(pp) > 10 {
			pp = pp[:10]
		}
		push(SplittedAnnualIndexRanking{TTKey{rawTopten.Year, index, 0}, append([]Page{}, pp...)})
	}

	//Add all empty values to eventually generate empty topten pages
	for year := range years {
		for index := range indexes {
			for topic := range topics {
				push(SplittedAnnualIndexRanking{TTKey{year, index, topic}, []Page{}})
			}
		}
	}

	return
}
