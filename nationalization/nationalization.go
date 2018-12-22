//go:generate go-bindata -pkg $GOPACKAGE -prefix "languages/" languages/...

package nationalization

import (
	"encoding/json"

	"github.com/ebonetti/overpedia/internal/preprocessor"
)

//New return a Nationalization in the specified language if it does exist.
func New(lang string) (data preprocessor.Nationalization, err error) {
	bytes, err := Asset(lang + ".json")
	if err != nil {
		return
	}

	data.Article2Topic = map[uint32]uint32{}

	if err = json.Unmarshal(bytes, &data); err != nil {
		data = preprocessor.Nationalization{}
	}

	return
}
