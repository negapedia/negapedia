package preprocessor

import (
	"context"
	"io/ioutil"
	"os"

	"github.com/negapedia/wikibrief"

	"github.com/RoaringBitmap/roaring"

	"github.com/ebonetti/ctxutils"
	"github.com/negapedia/wikiassignment"
	"github.com/negapedia/wikiassignment/nationalization"
)

func Run(ctx context.Context, CSVDir, lang string, noTFIDF bool, test bool) (err error) {
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

	p := preprocessor{nationalization, CSVDir, tmpDir, fail}

	articles := wikibrief.New(ctx, fail, tmpDir, lang, test)

	err = p.exportCSV(ctx, articles)

	return
}

type preprocessor struct {
	nationalization.Nationalization
	CSVDir, TmpDir string
	Fail           func(error) error
}
