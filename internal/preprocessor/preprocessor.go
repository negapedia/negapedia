package preprocessor

import (
	"context"
	"io/ioutil"
	"os"
	"sync"

	"github.com/negapedia/wikibrief"

	"github.com/RoaringBitmap/roaring"

	"github.com/ebonetti/ctxutils"
	"github.com/negapedia/wikiassignment"
	"github.com/negapedia/wikiassignment/nationalization"
)

type Process func(ctx context.Context, fail func(error) error, articles <-chan wikibrief.EvolvingPage)

func Run(ctx context.Context, CSVDir, lang string, test bool, processors ...Process) (err error) {
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

	processors = append([]Process{preprocessor{nationalization, CSVDir, tmpDir, fail}.exportCSV}, processors...)

	articlesChs := wikibrief.FanOut(ctx, wikibrief.New(ctx, fail, tmpDir, lang, test), len(processors))

	var wg sync.WaitGroup
	for i, p := range processors {
		wg.Add(1)
		go func(i int, p Process) {
			defer wg.Done()
			p(ctx, fail, articlesChs[i])
		}(i, p)
	}
	wg.Wait()

	return
}

type preprocessor struct {
	nationalization.Nationalization
	CSVDir, TmpDir string
	Fail           func(error) error
}
