package preprocessor

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/ebonetti/wikibrief"
	"github.com/gocarina/gocsv"
	"github.com/pkg/errors"
)

func (p preprocessor) exportCSV(ctx context.Context, articles <-chan article, botBlacklist map[uint32]string) (err error) {
	csvArticleRevisionChan := make(chan interface{}, 10000)

	//pages: topics and articles
	csvPageChan := make(chan interface{}, 10000)

	//social jumps input
	articleMultiEdgeChan := make(chan multiEdge, 10000)

	//social jumps output
	articleSocialJumpsChan := p.bi2Similgraph(ctx, articleMultiEdgeChan)

	go func() {
		defer close(csvArticleRevisionChan)
		defer close(csvPageChan)
		defer close(articleMultiEdgeChan)

		for _, t := range p.Topics { //dump topics
			select {
			case csvPageChan <- &csvPage{ID: t.ID, Title: t.Title}:
			//proceed
			case <-ctx.Done():
				return
			}
		}

		pageIds := roaring.NewBitmap()
		for a := range articles {
			//dumps may contains spurious duplicate of the same page and empty pages, that must be removed
			if pageIds.Contains(a.ID) || len(a.Revisions) == 0 {
				continue
			}
			pageIds.Add(a.ID)

			users2weight := make(map[uint32]float64, len(a.Revisions))
			revisions := transform(a, botBlacklist)
			for serialRevisionID, r := range revisions {
				//Export to csv
				if !p.FilterBots || !r.IsBot {
					csvArticleRevisionChan <- &revisions[uint32(serialRevisionID)]
				}

				//Convert data for socialjumps
				if r.IsBot || r.UserID == nil {
					continue //do not use for social jumps calculations
				}

				userID := *r.UserID
				userWeight := users2weight[userID]
				switch {
				case r.IsRevert > 0 || r.IsReverted:
					users2weight[userID] = math.Max(userWeight, 1.0)
				case r.Diff <= 100.0: //&& isPositive
					users2weight[userID] = math.Max(userWeight, 10.0)
				case userWeight <= 10:
					userWeight = 0 //Resetting weight for different scheme.
					fallthrough
				default:
					users2weight[userID] = math.Min(userWeight+r.Diff/10, 100)
				}
			}

			csvPageChan := csvPageChan
			articleMultiEdgeChan := articleMultiEdgeChan
			for i := 0; i < 2; i++ {
				select {
				case csvPageChan <- &csvPage{a.ID, a.Title, a.Abstract, a.TopicID}:
					csvPageChan = nil
				case articleMultiEdgeChan <- multiEdge{a.ID, users2weight}:
					articleMultiEdgeChan = nil
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	doneArticleRevisionWriting := make(chan interface{})
	go func() {
		defer close(doneArticleRevisionWriting)
		if err := chan2csv(csvArticleRevisionChan, filepath.Join(p.CSVDir, "revisions.csv")); err != nil {
			p.Fail(err)
		}
	}()

	if err = chan2csv(csvPageChan, filepath.Join(p.CSVDir, "pages.csv")); err != nil {
		return
	}

	//pages social jumps
	csvSocialJumpsChan := make(chan interface{}, 1000)
	go func() {
		defer close(csvSocialJumpsChan)
		for sj := range articleSocialJumpsChan {
			select {
			case csvSocialJumpsChan <- &csvSocialJumps{sj.From, uint32s(sj.To)}:
				//proceed
			case <-ctx.Done():
				return
			}
		}
	}()

	if err = chan2csv(csvSocialJumpsChan, filepath.Join(p.CSVDir, "socialjumps.csv")); err != nil {
		return
	}

	<-doneArticleRevisionWriting

	return
}

func transform(article article, botBlacklist map[uint32]string) (revisions []csvRevision) {
	revisions = make([]csvRevision, len(article.Revisions))

	oldWeight := float64(0)
	SHA12ID := make(map[string]uint32, len(article.Revisions))
	for ID, r := range article.Revisions {
		ID := uint32(ID)

		//User data
		var userID *uint32
		if uID := r.UserID; uID != wikibrief.AnonimousUserID {
			userID = &uID
		}
		_, isBot := botBlacklist[r.UserID]

		//Revision metric data
		diff := r.Weight - oldWeight
		oldWeight = r.Weight

		//revert count data
		IsRevert := uint32(0)
		oldID, isRevert := SHA12ID[r.SHA1]
		switch {
		case isRevert:
			IsRevert = ID - (oldID + 1)
			fallthrough
		case len(r.SHA1) == 31:
			SHA12ID[r.SHA1] = ID
		}

		revisions[ID] = csvRevision{article.ID, ID, userID, isBot, r.Weight, diff, IsRevert, true, r.Timestamp.Format(time.RFC3339Nano)}
	}

	//Add reverted data
	for ID := len(revisions) - 1; ID >= 0; ID -= 1 + int(revisions[ID].IsRevert) {
		revisions[ID].IsReverted = false
	}

	return
}

func chan2csv(c <-chan interface{}, filePath string) (err error) {
	var csvFile *os.File
	if csvFile, err = os.Create(filePath); err != nil {
		err = errors.Wrapf(err, "Error while creating file at %v", filePath)
		return
	}
	defer func() {
		if e := csvFile.Close(); e != nil && err == nil {
			err = errors.Wrapf(e, "Error while closing file %v", filePath)
		}
	}()

	bw := bufio.NewWriter(csvFile)
	defer bw.Flush()

	csvw := csv.NewWriter(bw)
	defer csvw.Flush()

	if err = gocsv.MarshalChan(c, gocsv.NewSafeCSVWriter(csvw)); err != nil {
		err = errors.Wrapf(err, "Error while marshaling to file %v", filePath)
	}
	return err
}

type csvRevision struct {
	PageID     uint32  `csv:"pageid"`
	ID         uint32  `csv:"ID"`
	UserID     *uint32 `csv:"userid"`
	IsBot      bool    `csv:"isbot"`
	Weight     float64 `csv:"weight"`
	Diff       float64 `csv:"diff"`
	IsRevert   uint32  `csv:"isrevert"`
	IsReverted bool    `csv:"isreverted"`
	Timestamp  string  `csv:"timestamp"`
}

type csvPage struct {
	ID       uint32 `csv:"id"`
	Title    string `csv:"title"`
	Abstract string `csv:"abstract"`
	TopicID  uint32 `csv:"topicid"`
}

type csvSocialJumps struct {
	ID          uint32  `csv:"id"`
	SocialJumps uint32s `csv:"socialjumps"`
}

type uint32s []uint32

func (s uint32s) String() string {
	pps := make([]string, len(s))
	for i, p := range s {
		pps[i] = fmt.Sprint(p)
	}
	return "{" + strings.Join(pps, ", ") + "}"
}
