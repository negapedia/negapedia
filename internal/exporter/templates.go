package exporter

import (
	"html/template"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"regexp"
	"time"

	"github.com/pkg/errors"

	"github.com/ebonetti/overpedia/nationalization"
)

var templates *template.Template

func init() {
	var err error
	if templates, err = templatesFromAssets("templates"); err != nil {
		panic(err)
	}
	if err = addHomepages(templates, "negapedia.org"); err != nil {
		panic(err)
	}
}

const (
	gchartImport         = "<script type=\"text/javascript\" src=\"https://www.gstatic.com/charts/loader.js\"></script>"
	baseHomepageTemplate = gchartImport + "\n{{template \"homepage.html\" .}}"
	pattern              = "(?s:" + gchartImport + ".*?</script>)"
)

func addHomepages(t *template.Template, baseDomain string) (err error) {
	r := regexp.MustCompile(pattern)

	var webpage []byte

	defaultHomepageTemplate := baseHomepageTemplate
	//Default to English homepage template if exist
	webpage, err = stubbornGet("http://en." + baseDomain)
	switch {
	case err != nil:
		return
	case !r.Match(webpage):
		return errors.New("English negapedia homepage doesn't seem to contain graph data")
	default:
		defaultHomepageTemplate = r.ReplaceAllString(string(webpage), baseHomepageTemplate)
	}

	for _, lang := range nationalization.List() {
		homepageTemplate := ""
		webpage, err = stubbornGet("http://" + lang + "." + baseDomain)
		switch {
		case err != nil:
			return
		case !r.Match(webpage):
			homepageTemplate = defaultHomepageTemplate
		default:
			homepageTemplate = r.ReplaceAllString(string(webpage), baseHomepageTemplate)
		}

		if _, err = t.New(nameHomepage(lang)).Parse(homepageTemplate); err != nil {
			return
		}
	}
	return
}

func nameHomepage(lang string) string {
	return lang + "homepage.html"
}

func stubbornGet(query string) (body []byte, err error) {
	for t := time.Second; t < time.Minute; t = t * 2 { //exponential backoff
		if body, err = get(query); err == nil {
			return
		}
	}
	return
}

func get(query string) (body []byte, err error) {
	resp, err := http.Get(query)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	return ioutil.ReadAll(resp.Body)
}

func templatesFromAssets(dir string) (t *template.Template, err error) {
	filenames, err := AssetDir(dir)
	if err != nil {
		return
	}
	for _, filename := range filenames {
		s := string(MustAsset(filepath.Join(dir, filename)))
		name := filepath.Base(filename)
		var tmpl *template.Template
		if t == nil {
			t = template.New(name)
		}
		if name == t.Name() {
			tmpl = t
		} else {
			tmpl = t.New(name)
		}
		if _, err = tmpl.Parse(s); err != nil {
			return nil, err
		}
	}
	return
}
