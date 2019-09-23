package exporter

import (
	"context"

	"github.com/pkg/errors"
)

//go:generate gorewrite

//ExtData represents external data. The underlying type of the interface must be of type map.
type ExtData struct {
	ID     uint32
	Fields map[string]interface{}
}

func (v1 ExtData) Less(v2 ExtData) bool {
	return v1.ID < v2.ID
}

//externalDataAdapter transforms extDataChannels into a loader function. Each channel must by ordered by increasing ID order.
func externalDataAdapter(ctx context.Context, fail func(error) error, extDataChannels []<-chan ExtData) (load func(e *Info)) {
	//Default value
	load = func(e *Info) {}

	if len(extDataChannels) == 0 { //No external data
		return
	}

	nexts := []func() (data ExtData, ok bool){}
	for _, ch := range extDataChannels {
		ch, oldData := ch, ExtData{}
		nexts = append(nexts, func() (data ExtData, ok bool) {
			select {
			case <-ctx.Done():
				return
			case data, ok = <-ch:
				//Go on
			}

			switch {
			case !ok:
				//Skip it
			case oldData.ID > data.ID:
				fail(errors.Errorf("Channel should be ordered in increasing order by ID, but %v > %v", oldData, data))
				data, ok = ExtData{}, false
			default:
				oldData = data
			}

			return
		})
	}

	merger := extdataIterMergeFrom(nexts...)

	oldID := uint32(0)
	return func(i *Info) {
		if oldID > i.Page.ID {
			fail(errors.Errorf("Load should be called on info ordered in increasing order by ID, but %v > %v", oldID, i.Page.ID))
			return
		}
		oldID = i.Page.ID

		for edata, ok := merger.Peek(); ok && edata.ID <= i.Page.ID; edata, ok = merger.Peek() {
			merger.Next()
			if edata.ID < i.Page.ID {
				continue
			}
			for field, info := range edata.Fields {
				i.ExternalFields[field] = info
			}
		}
	}
}
