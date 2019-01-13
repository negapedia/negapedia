package exporter

import (
	"html/template"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"regexp"

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
	for _, lang := range nationalization.List() {
		homepageTemplate := baseHomepageTemplate
		if webpage, err := get("http://" + lang + "." + baseDomain); err == nil {
			//replace old data with template
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
