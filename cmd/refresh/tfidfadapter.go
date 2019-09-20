package main

import (
	"context"

	"github.com/negapedia/negapedia/internal/exporter"
	"github.com/negapedia/wikitfidf"
	"github.com/pkg/errors"
)

func TFIDFExporter(ctx context.Context, fail func(error) error, tfidf wikitfidf.Exporter) (outs []<-chan exporter.ExtData) {
	if tfidf.Lang == "" { //invalid, TFIDF data is optional
		return
	}

	{
		globalWords, err := tfidf.GlobalWords()
		if err != nil {
			fail(errors.Wrap(err, "Error while Retrieving GlobalWords"))
			outs = nil
			return
		}
		out := make(chan exporter.ExtData, 1)
		outs = append(outs, out)
		out <- exporter.ExtData{0, map[string]interface{}{"Word2Occur": globalWords}}
		close(out)
	}

	{
		in := tfidf.Topics(ctx, fail)
		out := make(chan exporter.ExtData, 1)
		outs = append(outs, out)
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case p, ok := <-in:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case out <- exporter.ExtData{p.TopicID, map[string]interface{}{"Word2Occur": p.Words}}:
						//Go on
					}
				}
			}
		}()
	}

	{
		in := tfidf.Pages(ctx, fail)
		out := make(chan exporter.ExtData, 1)
		outs = append(outs, out)
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case p, ok := <-in:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case out <- exporter.ExtData{p.ID, map[string]interface{}{"Word2TFIDF": p.Words}}:
						//Go on
					}
				}
			}
		}()
	}

	{
		in := tfidf.BadwordsReport(ctx, fail)
		out := make(chan exporter.ExtData, 1)
		outs = append(outs, out)
		go func() {
			defer close(out)
			for {
				select {
				case <-ctx.Done():
					return
				case p, ok := <-in:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case out <- exporter.ExtData{p.PageID, map[string]interface{}{"BWord2Occur": p.BadW}}:
						//Go on
					}
				}
			}
		}()
	}

	return
}
