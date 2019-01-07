package exporter

import (
	"context"
	"path/filepath"
	"strings"

	"github.com/ebonetti/ctxutils"
)

//go:generate go-bindata -pkg $GOPACKAGE -prefix ".*/wiki2overpediadb/"  ../../../wiki2overpediadb/db/... templates/...
//go:generate gorewrite

func Walk(ctx context.Context, m Model, walkFn filepath.WalkFunc) (err error) {
	v, err := newView(m)
	if err != nil {
		return err
	}

	ctx, fail := ctxutils.WithFail(ctx)
	defer func() {
		if fe := fail(err); fe != nil {
			err = fe
		}
	}()

	next := v.FileIterator(ctx, fail)

	unwanted := ""
	for f, ok := next(); ok; f, ok = next() {
		if len(unwanted) > 0 && strings.HasPrefix(f.Name(), unwanted) {
			continue //skip it
		}
		unwanted = ""

		if err := walkFn(f.Name(), f, nil); err != nil {
			if f.IsDir() && err == filepath.SkipDir {
				unwanted = f.Name()
			} else {
				fail(err)
				break
			}
		}
	}
	return
}
