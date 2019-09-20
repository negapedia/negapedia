package exporter

import (
	"html/template"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"time"

	"github.com/pkg/errors"
)

func templates(baseURL url.URL) (templates *template.Template, err error) {
	if templates, err = templatesFromAssets("templates"); err != nil {
		return
	}
	if err = addHomepage(templates, baseURL); err != nil {
		templates = nil
	}
	return
}

const (
	gchartImport         = "<script type=\"text/javascript\" src=\"https://www.gstatic.com/charts/loader.js\"></script>"
	baseHomepageTemplate = gchartImport + "\n{{template \"homepagedata.html\" .}}"
	pattern              = "(?s:" + gchartImport + ".*?</script>)"
)

func addHomepage(t *template.Template, baseURL url.URL) (err error) {
	r := regexp.MustCompile(pattern)

	homepageTemplate := ""
	webpage, err := stubbornGet(baseURL.String())
	switch {
	case err != nil:
		homepageTemplate = baseHomepageTemplate
	case !r.Match(webpage):
		err = errors.New("Invalid homepage: old data not found in homepage")
	default:
		homepageTemplate = r.ReplaceAllString(string(webpage), baseHomepageTemplate)
	}

	if _, err = t.New("homepage.html").Parse(homepageTemplate); err != nil {
		return
	}

	return
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

	//Return if it's a dial tcp lookup error
	if err, ok := err.(*url.Error); ok {
		if err, ok := err.Err.(*net.OpError); ok {
			if _, ok := err.Err.(*net.DNSError); ok {
				return nil, nil
			}
		}
	}

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
