package nationalization

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	"github.com/ebonetti/overpedia/nationalization/internal"
)

//New return a Nationalization in the specified language if it does exist.
func New(lang string) (data Nationalization, err error) {
	bytes, err := internal.Asset(lang + ".json")
	if err != nil {
		err = errors.Wrapf(err, "Language %s not found", lang)
		return
	}

	d := data
	if err = json.Unmarshal(bytes, &d); err != nil {
		err = errors.Wrapf(err, "Error while parsing %s json", lang)
	} else {
		data = d
	}

	return
}

//List return the existing Nationalization languages.
func List() (langs []string) {
	for _, lang := range internal.AssetNames() {
		langs = append(langs, strings.TrimSuffix(lang, ".json"))
	}
	return
}

//Nationalization represents a Nationalization, the data that binds Negapedia to Wikipedia
type Nationalization struct {
	Language string
	Topics   []struct {
		Page
		Abstract             string
		Categories, Articles []Page `json:",omitempty"`
	}
	Filters []Page `json:",omitempty"`
}

//Page represents a page, consisting of a title and a page ID
type Page struct {
	ID    uint32
	Title string
}
