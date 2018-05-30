package exporter

import (
	"strings"
	"unicode"
)

var urlsRules = strings.NewReplacer(" ", "_", "/", "∕" /*<-- http://www.fileformat.info/info/unicode/char/2215/index.htm*/)

func pageUrl(p Page) string {
	switch {
	case len(p.Title) == 0 && p.IsTopic: //homepage
		return "../index.html"
	case p.IsTopic:
		return "../categories/" + urlsRules.Replace(p.Title) + ".html"
	default:
		return "../articles/" + urlsRules.Replace(p.Title) + ".html"
	}
}

func pageType(p Page) string {
	switch {
	case len(p.Title) == 0 && p.IsTopic: //homepage
		return "homepage"
	case p.IsTopic:
		return "topic"
	default:
		return "article"
	}
}

type page struct {
	Page
	Url, Type string
}

func pageList(pages ...Page) (pp []page) {
	pp = make([]page, len(pages))
	for i, p := range pages {
		p.Abstract = smartTruncate(p.Abstract, 256)
		pp[i] = page{p, pageUrl(p), pageType(p)}
	}
	return
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
