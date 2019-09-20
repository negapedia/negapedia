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

	"github.com/gocarina/gocsv"
	"github.com/negapedia/wikibrief"
	"github.com/pkg/errors"
)

func (p preprocessor) exportCSV(ctx context.Context, fail func(error) error, articles <-chan wikibrief.EvolvingPage) {
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

		for a := range articles {
			users2weight := make(map[uint32]float64, len(a.Revisions))
			serialRevisionID := uint32(0)
			oldWeight := float64(0)
			for r := range a.Revisions {
				serialRevisionID++

				//User data
				var userID *uint32
				if uID := r.UserID; uID != wikibrief.AnonimousUserID {
					userID = &uID
				}

				//Revision metric data
				weight := float64(len(r.Text))
				diff := weight - oldWeight
				oldWeight = weight

				//Export to csv
				csvArticleRevisionChan <- &csvRevision{a.PageID, serialRevisionID, userID, r.IsBot, weight, diff, r.IsRevert, false, r.Timestamp.Format(time.RFC3339Nano)}

				//Convert data for socialjumps
				if r.IsBot || r.UserID == wikibrief.AnonimousUserID {
					continue //do not use for social jumps calculations
				}

				userWeight := users2weight[r.UserID]
				switch {
				case r.IsRevert > 0:
					users2weight[r.UserID] = math.Max(userWeight, 1.0)
				case diff <= 100.0: //&& isPositive
					users2weight[r.UserID] = math.Max(userWeight, 10.0)
				case userWeight <= 10:
					userWeight = 0 //Resetting weight for different scheme.
					fallthrough
				default:
					users2weight[r.UserID] = math.Min(userWeight+diff/10, 100)
				}
			}

			csvPageChan := csvPageChan
			articleMultiEdgeChan := articleMultiEdgeChan
			for i := 0; i < 2; i++ {
				select {
				case csvPageChan <- &csvPage{a.PageID, a.Title, a.Abstract, a.TopicID}:
					csvPageChan = nil
				case articleMultiEdgeChan <- multiEdge{a.PageID, users2weight}:
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

	if err := chan2csv(csvPageChan, filepath.Join(p.CSVDir, "pages.csv")); err != nil {
		fail(err)
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

	if err := chan2csv(csvSocialJumpsChan, filepath.Join(p.CSVDir, "socialjumps.csv")); err != nil {
		fail(err)
		return
	}

	<-doneArticleRevisionWriting

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
