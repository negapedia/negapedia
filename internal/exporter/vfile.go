package exporter

import (
	"os"
	"time"

	"golang.org/x/text/collate"
	"golang.org/x/text/language"
)

func newVFile(name string, data []byte) vFile {
	return vFile{vEntity{name}, int64(len(data)), data}
}

type vFile struct {
	vEntity
	size int64
	data []byte
}

func (f vFile) Data() []byte {
	return f.data
}

func (f vFile) Size() int64 {
	return f.size
}

func (vFile) IsDir() bool {
	return false
}

var (
	fileMode    = os.FileMode(0755)
	modTime     = time.Now()
	enUSCollate = collate.New(language.MustParse("en_US"))
)

type vEntity struct {
	name string
}

func (e vEntity) Name() string {
	return e.name
}

func (e1 vEntity) Less(e2 interface{ Name() string }) bool {
	return enUSCollate.CompareString(e1.Name(), e2.Name()) == -1
}

func (vEntity) Mode() os.FileMode {
	return fileMode
}

func (vEntity) ModTime() time.Time {
	return modTime
}

func (vEntity) Sys() interface{} {
	return nil
}
