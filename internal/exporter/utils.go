package exporter

import (
	"strings"
	"unicode"

	"github.com/ebonetti/overpedia/nationalization"
)

var Topic topicData

func init() {
	type langID struct {
		Lang string
		ID   uint32
	}
	type title struct{ Title, FullTitle string }
	ID2Title := map[langID]title{}
	for _, lang := range nationalization.List() {
		data, err := nationalization.New(lang)
		if err != nil {
			panic(err)
		}
		for _, topic := range data.Topics {
			ID2Title[langID{lang, topic.ID}] = title{strings.Split(topic.Title, " ")[0], topic.Title}
		}
	}

	Topic = func(lang string, ID uint32) (topic, fullTopic string) {
		t := ID2Title[langID{lang, ID}]
		return t.Title, t.FullTitle
	}
}

type topicData func(lang string, ID uint32) (topic, fullTopic string)

func (d topicData) From(lang string, ID uint32) string {
	topic, _ := d(lang, ID)
	return topic
}

func (d topicData) FullFrom(lang string, ID uint32) string {
	_, topic := d(lang, ID)
	return topic
}

func (d topicData) UniversalFrom(ID uint32) string {
	topic, _ := d("en", ID)
	return topic
}

func (d topicData) UniversalFullFrom(ID uint32) string {
	_, topic := d("en", ID)
	return topic
}

var urlsRules = strings.NewReplacer(" ", "_", "/", "∕" /*<-- http://www.fileformat.info/info/unicode/char/2215/index.htm*/)

func pageUrl(p Page) string {
	switch p.Type {
	case "global": //homepage
		return "../index.html"
	case "topic":
		return "../categories/" + urlsRules.Replace(Topic.UniversalFullFrom(p.ParentID)) + ".html"
	default:
		return "../articles/" + urlsRules.Replace(p.Title) + ".html"
	}
}

func pageList(pages ...Page) []Page {
	for i, p := range pages {
		pages[i].Abstract = smartTruncate(p.Abstract, 256)
	}
	return pages
}

func smartTruncate(s string, limit int) string {
	if len(s) < limit {
		limit = len(s)
	}
	isNotOk := func(r rune) bool { return !unicode.In(r, unicode.L, unicode.M, unicode.N) }
	c := strings.LastIndexFunc(s[:limit], isNotOk)
	a := strings.LastIndexFunc(s[:c+1], func(r rune) bool { return !isNotOk(r) })
	if a == -1 {
		return ""
	}
	b := strings.IndexFunc(s[a:], isNotOk) + a //a < b <= c <= limit
	return s[:b]
}
