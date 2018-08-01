package exporter

import (
	"html/template"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"regexp"
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
	for _, lang := range []string{"aa", "ab", "ae", "af", "ak", "am", "an", "ar", "as", "av", "ay", "az", "ba", "be", "bg", "bh", "bi", "bm", "bn", "bo", "br", "bs", "ca", "ce", "ch", "co", "cr", "cs", "cu", "cv", "cy", "da", "de", "dv", "dz", "ee", "el", "en", "eo", "es", "et", "eu", "fa", "ff", "fi", "fj", "fl", "fo", "fr", "fy", "ga", "gd", "gl", "gn", "gu", "gv", "ha", "he", "hi", "ho", "hr", "ht", "hu", "hy", "hz", "ia", "id", "ie", "ig", "ii", "ik", "io", "is", "it", "iu", "ja", "jv", "ka", "kg", "ki", "kj", "kk", "kl", "km", "kn", "ko", "kr", "ks", "ku", "kv", "kw", "ky", "la", "lb", "lg", "li", "ln", "lo", "lt", "lu", "lv", "mg", "mh", "mi", "mk", "ml", "mn", "mr", "ms", "mt", "my", "na", "nb", "nd", "ne", "ng", "nl", "nn", "no", "nr", "nv", "ny", "oc", "oj", "om", "or", "os", "pa", "pi", "pl", "ps", "pt", "qu", "rm", "rn", "ro", "ru", "rw", "sa", "sc", "sd", "se", "sg", "si", "sk", "sl", "sm", "sn", "so", "sq", "sr", "ss", "st", "su", "sv", "sw", "ta", "te", "tg", "th", "ti", "tk", "tl", "tn", "to", "tr", "ts", "tt", "tw", "ty", "ug", "uk", "ur", "uz", "ve", "vi", "vo", "wa", "wo", "xh", "yi", "yo", "za", "zh", "zu"} {
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
