package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/ebonetti/wikipage"

	"github.com/ebonetti/overpedia/nationalization"
	"github.com/pkg/errors"
)

const (
	categoryNamespace = 14
	articleNamespace  = 0
)

func main() {
	for _, baseLang := range nationalization.List() {
		baseI18n := mynationalization(baseLang)
		for ti, t := range baseI18n.Topics {
			baseI18n.Topics[ti].Categories = nil
			for _, baseCategory := range t.Categories {
				for lang, category := range langlinks(baseLang, baseCategory, categoryNamespace) {
					i18n := mynationalization(lang)
					i18n.Topics[ti].Categories = add(i18n.Topics[ti].Categories, category)
				}
			}
			baseI18n.Topics[ti].Articles = nil
			for _, baseArticle := range t.Articles {
				for lang, article := range langlinks(baseLang, baseArticle, articleNamespace) {
					i18n := mynationalization(lang)
					i18n.Topics[ti].Articles = add(i18n.Topics[ti].Articles, article)
				}
			}
		}
		filters := baseI18n.Filters
		baseI18n.Filters = nil
		for _, baseFilter := range filters {
			for lang, filter := range langlinks(baseLang, baseFilter, categoryNamespace) {
				i18n := mynationalization(lang)
				i18n.Filters = add(i18n.Filters, filter)
			}
		}
	}

LOOP:
	for lang, i18n := range lang2NationalizationCache {
		for _, topic := range i18n.Topics {
			if len(topic.Categories) < 2 {
				continue LOOP
			}
		}
		if len(i18n.Filters) < 1 {
			continue LOOP
		}

		json, err := json.MarshalIndent(i18n, "", "  ")
		if err != nil {
			panic(err.Error())
		}

		err = ioutil.WriteFile(lang+".json", json, os.ModePerm)
		if err != nil {
			panic(err.Error())
		}
	}
}

func langlinks(lang string, from nationalization.Page, namespace int) (lang2Page map[string]nationalization.Page) {
	lang2Page = map[string]nationalization.Page{}

	const langLinksBase = "https://%v.wikipedia.org/w/api.php?action=query&prop=langlinks&lllimit=500&redirects&format=json&formatversion=2&titles=%v"
	page := get(queryFrom(langLinksBase, lang, []interface{}{from.Title}))
	switch {
	case page.Missing:
		fmt.Printf("Discarded missing %v in %v\n", from.Title, lang)
		return
	case page.Namespace != namespace:
		fmt.Printf("Discarded page %v from %v namespace - expected %v - in %v \n", from.Title, page.Namespace, namespace, lang)
		return
	case page.ID != from.ID:
		fmt.Printf("Changed ID of %v in %v\n", from.Title, lang)
	}

	lang2Page[lang] = nationalization.Page{ID: page.ID, Title: page.Title}

	for _, langLink := range page.LangLinks {
		p := get(queryFrom(langLinksBase, langLink.Lang, []interface{}{langLink.Title}))
		if p.Missing || p.Namespace != namespace {
			continue
		}
		lang2Page[langLink.Lang] = nationalization.Page{ID: p.ID, Title: p.Title}
	}

	return
}

var lang2NationalizationCache = map[string]*nationalization.Nationalization{}

func mynationalization(lang string) (result *nationalization.Nationalization) {
	result, ok := lang2NationalizationCache[lang]
	if ok {
		return
	}

	n, err := nationalization.New(lang)
	result = &n
	lang2NationalizationCache[lang] = result

	if err == nil {
		for _, t := range n.Topics {
			sortByID(t.Categories)
			sortByID(t.Articles)
		}
		sortByID(n.Filters)
	} else {
		n, _ = nationalization.New("en")
		n.Language = lang
		for i := range n.Topics {
			n.Topics[i].Categories = nil
			n.Topics[i].Articles = nil
		}
		n.Filters = nil
	}

	//Check if topic ID is already taken
	rh := wikipage.New(lang)
	for _, t := range n.Topics {
		p, err := rh.From(context.Background(), t.ID)
		_, ok := wikipage.NotFound(err)
		switch {
		case err == nil:
			panic(fmt.Errorf("In %v already exist a page with ID %v: %v", lang, t.ID, p))
		case !ok:
			panic(err)
			//default: //ok -> page do not exist
			//Do nothing
		}
	}

	return
}

func sortByID(pages []nationalization.Page) {
	sort.Slice(pages, func(i, j int) bool { return pages[i].ID < pages[j].ID })
}

func queryFrom(base string, lang string, infos []interface{}) (query string) {
	infoString := make([]string, len(infos))
	for i, info := range infos {
		infoString[i] = fmt.Sprint(info)
	}
	return fmt.Sprintf(base, lang, url.QueryEscape(strings.Join(infoString, "|")))
}

var _query2PageCache = map[string]mayMissingPage{}

func get(query string) (page mayMissingPage) {
	page, ok := _query2PageCache[query]
	if ok {
		return
	}

	for t := time.Second; t < time.Minute; t *= 2 { //exponential backoff
		pd, err := pagesDataFrom(query)
		switch {
		case err != nil:
			page.Missing = true
		case len(pd.Query.Pages) == 0:
			page.Missing = true
			return
		default:
			page = pd.Query.Pages[0]
			_query2PageCache[query] = page
			return
		}
		fmt.Println(err)
		time.Sleep(t)
	}

	return
}

type pagesData struct {
	Batchcomplete interface{}
	Warnings      interface{}
	Query         struct {
		Pages []mayMissingPage
	}
}

type mayMissingPage struct {
	ID        uint32 `json:"pageid"`
	Title     string
	Namespace int `json:"ns"`
	Missing   bool
	LangLinks []langLink
}

type langLink struct {
	Lang, Title string
}

var client = &http.Client{Timeout: time.Minute}

func pagesDataFrom(query string) (pd pagesData, err error) {
	fail := func(e error) (pagesData, error) {
		pd, err = pagesData{}, errors.Wrapf(e, "Error with the following query: %v", query)
		return pd, err
	}

	resp, err := client.Get(query)
	if err != nil {
		return fail(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fail(err)
	}

	err = json.Unmarshal(body, &pd)
	if err != nil {
		return fail(err)
	}

	if pd.Batchcomplete == nil {
		return fail(errors.Errorf("Incomplete batch with the following query: %v", query))
	}

	if pd.Warnings != nil {
		return fail(errors.Errorf("Warnings - %v - with the following query: %v", pd.Warnings, query))
	}

	return
}

func add(a []nationalization.Page, x nationalization.Page) []nationalization.Page {
	position := sort.Search(len(a), func(i int) bool { return a[i].ID >= x.ID })
	if exist := position < len(a) && a[position].ID == x.ID; exist {
		return a
	}

	a = append(a, nationalization.Page{})
	copy(a[position+1:], a[position:])
	a[position] = x

	return a
}
