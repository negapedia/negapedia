package nationalization

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"

	"github.com/ebonetti/overpedia/internal/preprocessor"
	"github.com/ebonetti/overpedia/nationalization/internal"
)

//New return a Nationalization in the specified language if it does exist.
func New(lang string) (data preprocessor.Nationalization, err error) {
	bytes, err := internal.Asset(lang + ".json")
	if err != nil {
		err = errors.Wrapf(err, "Language %s not found", lang)
		return
	}

	data.Article2Topic = map[uint32]uint32{}

	if err = json.Unmarshal(bytes, &data); err != nil {
		err = errors.Wrapf(err, "Error while parsing %s json", lang)
		data = preprocessor.Nationalization{}
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
