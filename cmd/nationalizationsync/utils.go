package main

import (
	//	"fmt"
	"sort"

	"github.com/ebonetti/overpedia/nationalization"
	"github.com/pkg/errors"
)

type i18lPage struct {
	Lang string
	nationalization.Page
}

func translatorFrom(next func() (p i18lPage, ok bool)) translator {
	pp := []i18lPage{}
	for p, ok := next(); ok; p, ok = next() {
		pp, _ = i18lPageAdd(pp, p)
	}
	return pp
}

type translator []i18lPage

func (t translator) ToID(p i18lPage) (ID uint32) {
	exist, intID := i18lPageExist(t, p)
	if !exist {
		panic(errors.Errorf("Translator: inexistent page %v", p))
	}
	ID = uint32(intID)
	return
}

func (t translator) ToPage(ID uint32) (p i18lPage) {
	if ID >= uint32(len(t)) {
		panic(errors.Errorf("Translator: inexistent ID %v", ID))
	}

	p = t[ID]
	return
}

func i18lPageSearch(pp []i18lPage, p i18lPage) int {
	return sort.Search(len(pp), func(i int) bool {
		p2 := pp[i]
		switch { //check p <= p2
		case p.Lang > p2.Lang:
			return false
		case p.Lang == p2.Lang && p.ID > p2.ID:
			return false
		default:
			return true
		}
	})
}

func i18lPageExist(a []i18lPage, x i18lPage) (exist bool, position int) {
	position = i18lPageSearch(a, x)
	exist = position < len(a) && a[position] == x
	return
}

func i18lPageAdd(a []i18lPage, x i18lPage) (result []i18lPage, existed bool) {
	exist, position := i18lPageExist(a, x)
	if exist {
		return a, true
	}

	a = append(a, i18lPage{})
	copy(a[position+1:], a[position:])
	a[position] = x

	return a, false
}
