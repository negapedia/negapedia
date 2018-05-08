package exporter

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
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

// CopyAssets copy an asset under the given directory recursively
func CopyAssets(write func(string, []byte) error, dir string) error {
	ac := assetCopier{Write: write}
	ac.copyAssets(dir)
	return ac.Err
}

type assetCopier struct {
	Err   error
	Write func(string, []byte) error
}

func (ac *assetCopier) copyAssets(dir string) {
	fmt.Println("copyAssets(", dir, ") - err : ", ac.Err) //////////////////////
	children, err := AssetDir(dir)
	// File
	if err != nil {
		ac.copyAsset(dir)
		return
	}
	// Dir
	for _, child := range children {
		ac.copyAssets(filepath.Join(dir, child))
	}
}

func (ac *assetCopier) copyAsset(name string) {
	fmt.Println("copyAsset(", name, ") - err : ", ac.Err) //////////////////////
	if ac.Err != nil {
		return
	}

	if data, err := Asset(name); err != nil {
		ac.Err = err
	} else {
		ac.Err = ac.Write(name, data)
	}
}

func WriteFile(filename string, b []byte, fMode os.FileMode) (err error) {
	/*defer func(){
	    fmt.Println("WriteFile(",filename,") - err : ",err)//////////////////////
	}()*/
	if err = os.MkdirAll(filepath.Dir(filename), fMode); err != nil {
		return err
	}
	if err = ioutil.WriteFile(filename, b, fMode); err != nil {
		return err
	}
	return nil
}

/*var _ordinalSuffixes = []string{"th", "st", "nd", "rd"}

func ordinal(n int) string {
	absN := n
	if n < 0 {
		absN = -n
	}
	absNMod10, absNMod100 := absN%10, absN%100
	suffixIndex := 0
	switch {
	case 10 <= absNMod100 && absNMod100 <= 20:
		//do nothing
	case absNMod10 < len(_ordinalSuffixes):
		suffixIndex = absNMod10
	}
	return fmt.Sprint(n, _ordinalSuffixes[suffixIndex])
}*/
