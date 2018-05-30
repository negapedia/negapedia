package exporter

import (
	"fmt"
)

func (v View) transformPage(i Info) interface{} {
	p := viewInfo(i)
	p.Page.Abstract = smartTruncate(p.Page.Abstract, 512)
	if p.Page.IsTopic && len(p.Links) > 3 {
		p.Links = p.Links[:3]
	}

	return &struct {
		mData
		CanonicalLink
		ExtendedPage
		Type     string
		Rankings []ranking
		Links    []page
	}{
		v.data,
		CanonicalLink{},
		p.Page,
		pageType(p.Page.Page),
		p.Rankings(),
		pageList(p.Links...),
	}
}

func (v View) transformHomePage(i Info) interface{} {
	p := viewInfo(i)
	return &struct {
		mData
		CanonicalLink
		Rankings []ranking
	}{
		v.data,
		CanonicalLink{},
		p.Rankings(),
	}
}

type viewInfo Info

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

func (p viewInfo) Rankings() (rankings []ranking) {
	for index, amm := range p.Index2Measurement {
		rankings = append(rankings, ranking{amm.Rank, amm.Percentile, amm.DensePercentile, index, "all", "all", amm.Value})
	}
	for index, ymm := range p.Index2YearMeasurements {
		for _, ym := range ymm {
			year := fmt.Sprint(ym.Year)
			rankings = append(rankings, ranking{ym.Rank, ym.Percentile, ym.DensePercentile, index, "all", year, ym.Value})
		}
	}

	if p.Page.IsTopic {
		return
	}

	for index, amm := range p.Index2Measurement {
		rankings = append(rankings, ranking{amm.TopicRank, amm.TopicPercentile, amm.TopicDensePercentile, index, p.Page.Topic, "all", amm.Value})
	}
	for index, ymm := range p.Index2YearMeasurements {
		for _, ym := range ymm {
			year := fmt.Sprint(ym.Year)
			rankings = append(rankings, ranking{ym.TopicRank, ym.TopicPercentile, ym.TopicDensePercentile, index, p.Page.Topic, year, ym.Value})
		}
	}
	return
}

func percentage(percentile float64) int {
	return int(percentile*100 + 0.5)
}

type CanonicalLink struct {
	CanonicalURL string
}

func (l *CanonicalLink) SetCanonicalLink(s string) {
	l.CanonicalURL = s
}
