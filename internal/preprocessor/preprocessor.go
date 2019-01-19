package preprocessor

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"runtime"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/remeh/sizedwaitgroup"

	"github.com/ebonetti/ctxutils"
	"github.com/ebonetti/overpedia/nationalization"
	"github.com/ebonetti/wikiassignment"
	"github.com/ebonetti/wikibots"
	"github.com/ebonetti/wikibrief"
	"github.com/ebonetti/wikidump"
	"github.com/ebonetti/wikipage"
)

func Run(ctx context.Context, CSVDir, lang string, filterBots bool) (err error) {
	ctx, fail := ctxutils.WithFail(ctx)
	defer func() {
		if fe := fail(err); fe != nil {
			err = fe
		}
	}()

	tmpDir, err := ioutil.TempDir(CSVDir, ".")
	if err != nil {
		return
	}
	defer os.RemoveAll(tmpDir)

	latestDump, err := wikidump.Latest(tmpDir, lang, "metahistory7zdump", "pagetable", "redirecttable", "categorylinkstable", "pagelinkstable")
	if err != nil {
		return
	}

	article2Topic, namespaces, err := wikiassignment.From(ctx, tmpDir, lang)
	if err != nil {
		return
	}

	//Filter out non articles
	articlesIDS := roaring.BitmapOf(namespaces.Articles...)
	for pageID := range article2Topic {
		if !articlesIDS.Contains(pageID) {
			delete(article2Topic, pageID)
		}
	}

	nationalization, err := nationalization.New(lang)
	if err != nil {
		return
	}

	p := preprocessor{nationalization, article2Topic, latestDump, CSVDir, tmpDir, filterBots, fail}

	botIDs2Name, err := wikibots.New(ctx, p.Language)
	if err != nil {
		return
	}

	articles := p.Articles(ctx)

	err = p.exportCSV(ctx, articles, botIDs2Name)

	return
}

type preprocessor struct {
	nationalization.Nationalization
	Article2Topic  map[uint32]uint32
	Dump           wikidump.Wikidump
	CSVDir, TmpDir string
	FilterBots     bool
	Fail           func(error) error
}

type article struct {
	wikipage.WikiPage
	TopicID   uint32
	Revisions []wikibrief.Revision
}

const nN = 200

func (p preprocessor) Articles(ctx context.Context) <-chan article {
	results := make(chan article, 2*nN)
	go func() {
		defer close(results)
		summaries := p.summaries(ctx, func(e uint32) (ok bool) {
			_, ok = p.Article2Topic[e] //is valid article
			return
		})
		wikiPage := wikipage.New(p.Language)
		wg := sync.WaitGroup{}
		for i := 0; i < nN; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
			loop:
				for s := range summaries {
					wp, err := wikiPage.From(ctx, s.PageID) //bottle neck - query to wikipedia api
					_, NotFound := wikipage.NotFound(err)
					switch {
					case NotFound:
						continue loop //Do nothing
					case err != nil:
						p.Fail(err)
						return
					}
					topicID, _ := p.Article2Topic[s.PageID]
					select {
					case results <- article{wp, topicID, s.Revisions}:
						//proceed
					case <-ctx.Done():
						return
					}
				}
			}()
		}
		wg.Wait()
	}()

	return results
}

func (p preprocessor) summaries(ctx context.Context, isArticle func(e uint32) (ok bool)) <-chan wikibrief.Summary {
	results := make(chan wikibrief.Summary, 2*nN)
	go func() {
		defer close(results)
		it := p.Dump.Open("metahistory7zdump")

		//limit the number of workers to prevent system from killing 7zip instances
		wg := sizedwaitgroup.New(10 * runtime.NumCPU())
		r, err := it(ctx)
		for ; err == nil; err = io.EOF { //Use just one dump file for testing purposes
			//for ; err == nil; r, err = it(ctx) {
			if err = wg.AddWithContext(ctx); err != nil {
				return //AddWithContext only fail if ctx is Done
			}
			go func(r io.ReadCloser) {
				defer wg.Done()
				defer func() {
					if err := r.Close(); err != nil {
						p.Fail(err)
					}
				}()
				it := wikibrief.New(r, isArticle, func(text string) float64 { return float64(len(text)) })
				s, err := it()
				for ; err == nil; s, err = it() {
					select {
					case results <- s:
						//proceed
					case <-ctx.Done():
						return
					}
				}
				switch err {
				case nil:
					//Do nothing
				case io.EOF:
					//Do nothing
				default:
					p.Fail(err)
				}
			}(r)
		}
		switch err {
		case nil:
			//Do nothing
		case io.EOF:
			//Do nothing
		default:
			p.Fail(err)
		}
		wg.Wait()
	}()

	return results
}
