package exporter

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"
)

func (v View) transformTopTen(r SplittedAnnualIndexRanking) interface{} {
	title := fmt.Sprint("Top Ten - ", strings.Title(r.Index))
	topic := "all"
	if r.TopicID != 0 {
		topic = Topic.From(v.model.Lang(), r.TopicID)
		title += " - " + topic
	}
	span := fmt.Sprint(r.Year)
	if r.Year == 0 {
		span = "all"
	} else {
		title += " - " + span
	}

	for i, p := range r.Ranking {
		r.Ranking[i].Abstract = smartTruncate(p.Abstract, 512)
	}

	return &struct {
		Lang string
		CanonicalLink
		Title, Topic, Span string
		SplittedAnnualIndexRanking
	}{
		v.model.Lang(),
		CanonicalLink{},
		title,
		topic,
		span,
		r,
	}
}

func topTenUrl(k TTKey) string {
	year := fmt.Sprint(k.Year)
	if k.Year == 0 {
		year = "all"
	}
	topic := Topic.UniversalFrom(k.TopicID)
	if k.TopicID == 0 {
		topic = "all"
	}

	return path.Join("..", "toptens", year, k.Index, topic+".html")
}

type TTKey struct {
	Year    uint32 //0 iff it's all time
	Index   string
	TopicID uint32 //0 iff it's all
}

type SplittedAnnualIndexRanking struct {
	TTKey
	Ranking []Page
}

func (v View) splittedTopTen(ctx context.Context) (rankings []SplittedAnnualIndexRanking, err error) {
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

	//Fetch data from model
	err = v.model.TopTen(ctx, func(r AnnualIndexesRanking) error {
		for index, pp := range r.Index2Ranking {
			for _, p := range pp {
				push(SplittedAnnualIndexRanking{TTKey{r.Year, index, p.ParentID}, []Page{p}})
			}
			if len(pp) > 10 {
				pp = pp[:10]
			}
			push(SplittedAnnualIndexRanking{TTKey{r.Year, index, 0}, append([]Page{}, pp...)})
		}
		return nil
	})

	//Add all empty values to eventually generate empty topten pages
	for year := range years {
		for index := range indexes {
			for topic := range topics {
				push(SplittedAnnualIndexRanking{TTKey{year, index, topic}, []Page{}})
			}
		}
	}

	if err != nil {
		rankings = []SplittedAnnualIndexRanking{}
	}

	return
}
