package exporter

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
)

func (v View) transformTopTen(r SplittedAnnualIndexRanking) interface{} {
	title := fmt.Sprint("Top Ten of ", strings.Title(r.Index))
	if r.Topic != "all" {
		title += " for " + v.topics[r.Topic].Title
	}
	span := fmt.Sprint(r.Year)
	if r.Year == 0 {
		span = "all"
	} else {
		title += " in " + span
	}

	for i, p := range r.Ranking {
		r.Ranking[i].Abstract = smartTruncate(p.Abstract, 512)
	}

	return &struct {
		Lang string
		CanonicalLink
		Title, Span string
		SplittedAnnualIndexRanking
	}{
		v.data.Lang,
		CanonicalLink{},
		title,
		span,
		r,
	}
}

func topTenUrl(k TTKey) string {
	year := fmt.Sprint(k.Year)
	if k.Year == 0 {
		year = "all"
	}

	return path.Join("..", "toptens", year, k.Index, k.Topic+".html")
}

type TTKey struct {
	Year  int //0 iff it's all time
	Index string
	Topic string //may be "all"
}

type SplittedAnnualIndexRanking struct {
	TTKey
	Ranking []Page
}

func (v View) splittedTopTen(ctx context.Context) (rankings []SplittedAnnualIndexRanking, err error) {
	push := func(r SplittedAnnualIndexRanking) {
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
			case ri.Topic < r.Topic:
				return false
			default: //case ri.Topic >= r.Topic:
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

	//Add all empty values ahead to eventually generate empty topten pages
	years := []int{0}
	for year := v.data.BoundingYears.Min; year <= v.data.BoundingYears.Max; year++ {
		years = append(years, year)
	}
	topics := []string{"all"}
	for _, topic := range v.data.Topics {
		topics = append(topics, topic.Topic)
	}
	for _, year := range years {
		for _, index := range v.data.Indexes {
			for _, topic := range topics {
				push(SplittedAnnualIndexRanking{TTKey{year, index, topic}, []Page{}})
			}
		}
	}

	//Fetch data from model
	err = v.model.TopTen(ctx, func(r AnnualIndexesRanking) error {
		for index, pp := range r.Index2Ranking {
			for _, p := range pp {
				push(SplittedAnnualIndexRanking{TTKey{r.Year, index, p.Topic}, []Page{p}})
			}
			if len(pp) > 10 {
				pp = pp[:10]
			}
			push(SplittedAnnualIndexRanking{TTKey{r.Year, index, "all"}, append([]Page{}, pp...)})
		}
		return nil
	})

	if err != nil {
		rankings = []SplittedAnnualIndexRanking{}
	}

	return
}
