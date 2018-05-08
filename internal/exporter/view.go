package exporter

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"path"
	"path/filepath"
	"strings"
)

func newView(m Model) (v View, err error) {
	v.model = m
	v.data = m.Data()

	v.topics = make(map[string]Page, len(v.data.Topics))
	for _, t := range v.data.Topics {
		v.topics[t.Topic] = t
	}

	return
}

type View struct {
	model Model
	data  mData

	topics map[string]Page
}

func (v View) Transform(i interface{}) (info vFile, err error) {
	templateName, pagePath := "", ""
	switch t := i.(type) {
	case Info:
		pagePath = pageUrl(t.Page.Page)
		if len(t.Page.Title) == 0 {
			i = v.transformHomePage(t)
			templateName = nameHomepage(v.data.Lang)
		} else {
			i = v.transformPage(t)
			templateName = "page.html"
		}
	case SplittedAnnualIndexRanking: //topten
		i = v.transformTopTen(t)
		templateName = "topten.html"
		pagePath = topTenUrl(t.TTKey)
	default:
		err = fmt.Errorf("unexpected type %T", t)
	}
	if err != nil {
		return
	}
	var b bytes.Buffer

	reldir, file := path.Split(pagePath)
	file = url.PathEscape(strings.TrimSuffix(file, ".html"))
	i.(interface{ SetCanonicalLink(string) }).SetCanonicalLink("http://" + path.Join(v.data.Lang+".negapedia.org", "articles", reldir, file))

	err = templates.ExecuteTemplate(&b, templateName, i)
	info = newVFile(filepath.Join("overpedia.com", "articles", pagePath), b.Bytes())
	return
}

func (v View) FileIterator(ctx context.Context, fail func(error) error) func() (vFile, bool) {
	nexts := []func() (info vFile, ok bool){}
	cc := []<-chan vFile{v.topTenFiles(ctx, fail)}
	cc = append(cc, v.pageFiles(ctx, fail)...)
	for _, c := range cc {
		c := c
		nexts = append(nexts, func() (f vFile, ok bool) {
			f, ok = <-c
			return
		})
	}

	return vfileIterMergeFrom(nexts...).Next
}

func (v View) pageFiles(ctx context.Context, fail func(error) error) []<-chan vFile {
	pageDepth2Count := v.data.PageDepth2Count[:len(v.data.PageDepth2Count)-1]
	ffiles := make([]chan vFile, len(pageDepth2Count))
	for depth, count := range pageDepth2Count {
		ffiles[depth] = make(chan vFile, count)
	}
	ffiles = append(ffiles, make(chan vFile, 128)) //cap the capacity of the last channel

	go func() {
		depth := 0
		defer func() {
			for _, files := range ffiles[depth:] {
				close(files)
			}
		}()
		err := v.model.Pages(ctx, func(p Info) (err error) {
			info, err := v.Transform(p)
			if err != nil {
				return
			}

			if p.Page.PageDepth > depth {
				close(ffiles[depth])
				depth = p.Page.PageDepth
			}

			select {
			case ffiles[depth] <- info:
				return nil //go on
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err != nil {
			fail(err)
		}
	}()

	outffiles := make([]<-chan vFile, len(ffiles))
	for i, files := range ffiles {
		outffiles[i] = files
	}
	return outffiles
}

func buffer2chan(ctx context.Context, files chan<- vFile, buffer []vFile) {
	defer close(files)
	for _, info := range buffer {
		select {
		case files <- info:
			//go on
		case <-ctx.Done():
			return
		}
	}
}

func (v View) topTenFiles(ctx context.Context, fail func(error) error) <-chan vFile {
	files := make(chan vFile, 128)
	go func() {
		defer close(files)
		rankings, err := v.splittedTopTen(ctx)
		if err != nil {
			fail(err)
			return
		}
		for _, r := range rankings {
			info, err := v.Transform(r)
			if err != nil {
				fail(err)
				return
			}
			select {
			case files <- info:
				//go on
			case <-ctx.Done():
				return
			}
		}
	}()
	return files
}
