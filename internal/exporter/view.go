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
	return View{m}, nil
}

type View struct {
	model Model
}

func (v View) Transform(i interface{}) (info vFile, err error) {
	templateName, pagePath := "", ""
	switch t := i.(type) {
	case Info:
		pagePath = pageUrl(t.Page)
		i = v.transformPage(t)
		if len(t.Page.Title) == 0 {
			templateName = nameHomepage(v.model.Lang())
		} else {
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
	i.(interface{ SetCanonicalLink(string) }).SetCanonicalLink("http://" + path.Join(v.model.Lang()+".negapedia.org", "articles", reldir, file))

	err = templates.ExecuteTemplate(&b, templateName, i)
	info = newVFile(filepath.Join("html", "articles", pagePath), b.Bytes())
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

func (v View) pageFiles(ctx context.Context, fail func(error) error) (outffiles []<-chan vFile) {
	ffiles := []chan vFile{}
	for i := 0; i < 4; i++ {
		fchan := make(chan vFile, 128)
		ffiles = append(ffiles, fchan)
		outffiles = append(outffiles, fchan)
	}

	go func() {
		var lastPageType string
		defer func() {
			for _, files := range ffiles {
				close(files)
			}
		}()
		err := v.model.Pages(ctx, func(p Info) (err error) {
			info, err := v.Transform(p)
			if err != nil {
				return
			}

			if p.Page.Type != lastPageType {
				close(ffiles[0])
				ffiles = ffiles[1:]
				lastPageType = p.Page.Type
			}

			select {
			case ffiles[0] <- info:
				return nil //go on
			case <-ctx.Done():
				return ctx.Err()
			}
		})
		if err != nil {
			fail(err)
		}
	}()

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
