package exporter

import (
	"fmt"
	"html/template"
	"time"
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
		Charts   []chart
		Rankings []ranking
		Links    []page
	}{
		v.data,
		CanonicalLink{},
		p.Page,
		pageType(p.Page.Page),
		p.Charts(),
		p.Rankings(),
		pageList(p.Links...),
	}
}

func (v View) transformHomePage(i Info) interface{} {
	p := viewInfo(i)
	return &struct {
		mData
		CanonicalLink
		Charts []chart
	}{
		v.data,
		CanonicalLink{},
		p.Charts()[1:],
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

func (p viewInfo) Charts() []chart {
	return []chart{{"summary", p.AllCharts()}, {"history", p.YearCharts()}}
}

type chart struct {
	Span    string
	Indexes []chartIndex
}

type chartIndex struct {
	Index string
	Rows  [][]cell
}

type chartsData struct {
	Title   string
	Indexes []string
}

var cData = []chartsData{
	{"conflict", []string{"conflict"}},
	{"polemic", []string{"polemic"}},
}

func (p viewInfo) AllCharts() []chartIndex {
	charts := make([]chartIndex, len(cData))
	for i, c := range cData {
		mm := make([][]Measurement, len(c.Indexes))
		for i, index := range c.Indexes {
			i2mm := p.Index2YearMeasurements[index]
			mm[i] = []Measurement{i2mm[len(i2mm)-1].Measurement}
			//		    mm[i] = []Measurement{p.Index2Measurement[index]}
		}
		charts[i] = chartIndex{
			c.Title,
			measurements2Cells(allChartFormatter, mm...),
		}
	}
	return charts
}

func (p viewInfo) YearCharts() []chartIndex {
	lineChartFormatterBuilder := lineChartFormatterBuilder
	if pageType(p.Page.Page) == "homepage" {
		lineChartFormatterBuilder = homepageLineChartFormatterBuilder
	}

	charts := make([]chartIndex, len(cData))
	for i, c := range cData {
		mm := make([][]YearMeasurement, len(c.Indexes))
		for i, index := range c.Indexes {
			mm[i] = p.Index2YearMeasurements[index]
		}
		charts[i] = chartIndex{
			c.Title,
			yearMeasurements2Cells(lineChartFormatterBuilder, mm...),
		}
	}
	return charts
}

type cell struct {
	Value, FormattedValue, Percentile, Rank interface{}
}

func yearMeasurements2Cells(formatter func(m Measurement) cell, mm ...[]YearMeasurement) (rows [][]cell) {
	rows = make([][]cell, len(mm[0]))
	for j := range rows {
		rows[j] = make([]cell, len(mm)+1)
		year := mm[0][j].Year
		rows[j][0] = cell{Value: time2JS(time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)), FormattedValue: fmt.Sprint(year)}

		cells := rows[j][1:]
		for i := range cells {
			m := mm[i][j]
			cells[i] = formatter(m.Measurement)
		}
	}
	return
}

func time2JS(t time.Time) template.JS {
	return template.JS(fmt.Sprint("new Date(", t.Unix()*1000, ")"))
}

func measurements2Cells(formatter func(m Measurement) cell, mm ...[]Measurement) (rows [][]cell) {
	rows = make([][]cell, len(mm[0]))
	for j := range rows {
		cells := make([]cell, len(mm))
		rows[j] = cells
		for i := range cells {
			m := mm[i][j]
			cells[i] = formatter(m)
		}
	}
	return
}

func allChartFormatter(m Measurement) cell {
	v := myPercentage(m)
	return cell{Value: v, FormattedValue: fmt.Sprint(v)}
}

func lineChartFormatterBuilder(m Measurement) cell {
	return cell{m.Value, fmt.Sprint(int(m.Value + 0.5)), myPercentage(m), m.Rank}
}

func homepageLineChartFormatterBuilder(m Measurement) cell {
	return cell{Value: m.Value, FormattedValue: fmt.Sprint(int(m.Value + 0.5))}
}

func myPercentile(m Measurement) float64 {
	return m.DensePercentile
}

func percentage(percentile float64) int {
	return int(percentile*100 + 0.5)
}

func myPercentage(m Measurement) int {
	return percentage(myPercentile(m))
}

type CanonicalLink struct {
	CanonicalURL string
}

func (l *CanonicalLink) SetCanonicalLink(s string) {
	l.CanonicalURL = s
}
