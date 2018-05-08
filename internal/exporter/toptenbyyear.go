package exporter

import (
	"fmt"
	"sort"
	"path"
	"strings"
	"context"
)

func (v View) transformTopTen(r SplittedAnnualIndexRanking) interface{} {
    title := fmt.Sprint("Top Ten of ",strings.Title(r.Index))
    if r.Topic != "all" {
        title += " for " + v.topics[r.Topic].Title
    }
    span := fmt.Sprint(r.Year)
    if r.Year == 0 {
        span = "all"
    } else {
        title += " in " + span
    }
    
    for i,p := range r.Ranking {
        r.Ranking[i].Abstract = smartTruncate(p.Abstract, 512)
    }
    
	return &struct {
	    Lang string
	    CanonicalLink
		Title, Span    string
		SplittedAnnualIndexRanking
	} {
	    v.data.Lang,
	    CanonicalLink{},
		title,
		span,
		r,
	}
}

func topTenUrl(k TTKey) string {
    year := fmt.Sprint(k.Year)
    if k.Year == 0 {
        year = "all"
    }
    
    return path.Join("..", "toptens", year, k.Index, k.Topic+".html")
}

const (
    minUint uint = 0
    maxUint      = ^minUint
    maxInt       = int(maxUint >> 1)
    minInt       = ^maxInt
)

type TTKey struct {
    Year    int    //if  0 -> all
    Index   string
    Topic   string //may be all
}

type SplittedAnnualIndexRanking struct {//define sorting..
    TTKey
	Ranking []Page
}

func (v View) splittedTopTen(ctx context.Context) (rankings []SplittedAnnualIndexRanking, err error) {
    push := func(r SplittedAnnualIndexRanking) {
        p := sort.Search(len(rankings), func(i int) (ismoreorequal bool) {
            ri := rankings[i]
            switch {
            case ri.Year < r.Year:
                return false
            case ri.Year > r.Year:
                return true
            case ri.Index < r.Index:
                return false
            case ri.Index > r.Index:
                return true
            case ri.Topic < r.Topic:
                return false
            default: //case ri.Topic >= r.Topic:
                return true
            }
        })
        if p < len(rankings) && r.TTKey == rankings[p].TTKey {
            rankings[p].Ranking = append(rankings[p].Ranking,r.Ranking...)
            return
        }
        rks := append(rankings, SplittedAnnualIndexRanking{})
        copy(rks[p+1:],rks[p:])
        rks[p] = r        
        rankings = rks
    }
    
    err = v.model.TopTen(ctx, func(r AnnualIndexesRanking) error {
        if r.Year == 0 {
            r.Year = maxInt
        }
        for index, pp := range r.Index2Ranking {
            for _,p := range pp {
                push(SplittedAnnualIndexRanking{TTKey{r.Year,index,p.Topic},[]Page{p}})
            }
            if len(pp) > 10 {
                pp = pp[:10]
            }
            push(SplittedAnnualIndexRanking{TTKey{r.Year,index,"all"}, append([]Page{},pp...)})
        }
        return nil
    })
    
    for i,r := range rankings {
        if r.Year == maxInt {
            rankings[i].Year = 0
        }
    }
    
    if err != nil {
        rankings = []SplittedAnnualIndexRanking{}
    }
    
    return
}
