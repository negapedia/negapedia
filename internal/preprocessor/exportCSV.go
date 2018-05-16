package preprocessor

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/ebonetti/wikibrief"
	"github.com/gocarina/gocsv"
)

func (p preprocessor) exportCSV(ctx context.Context, articles <-chan article, botBlacklist map[uint32]string) (err error) {
	csvArticleRevisionChan := make(chan interface{}, 10000)

	//pages: topics and articles
	csvPageChan := make(chan interface{}, 10000)

	//social jumps input
	articleMultiEdgeChan := make(chan multiEdge, 10000)

	//social jumps output
	articleSocialJumpsChan := newBi2Similgraph(ctx, articleMultiEdgeChan, p.EstPages, p.EstUsers, p.EstEdits, p.Fail)

	go func() {
		defer close(csvArticleRevisionChan)
		defer close(csvPageChan)
		defer close(articleMultiEdgeChan)

		for _, t := range p.Topics { //dump topics
			select {
			case csvPageChan <- &csvPage{t.ID, t.Title, t.Abstract, t.WikipediaURL, t.ID, socialJumps{}}:
			//proceed
			case <-ctx.Done():
				return
			}
		}

		pageIds := roaring.NewBitmap()
		for a := range articles {
			//dumps may contains spurious duplicate of the same page, that must be removed
			if pageIds.Contains(a.ID) {
				continue
			}
			pageIds.Add(a.ID)

			InterestedUsers := []*roaring.Bitmap{roaring.NewBitmap(), roaring.NewBitmap(), roaring.NewBitmap()}
			oldWeight := float64(0)
			for _, r := range a.Revisions {
				ID := r.UserID
				userID := &ID

				_, isBot := botBlacklist[r.UserID]

				diff := r.Weight - oldWeight
				oldWeight = r.Weight

				switch {
				case r.UserID == wikibrief.AnonimousUserID:
					userID = nil
					isBot = false
				case isBot:
					//do nothing
				case r.IsRevert > 0 && diff <= 120:
					InterestedUsers[0].Add(r.UserID)
				case r.IsRevert > 0 || diff <= 120:
					InterestedUsers[1].Add(r.UserID)
				default:
					InterestedUsers[2].Add(r.UserID)
				}
				csvArticleRevisionChan <- &csvArticleEg{r.ID, userID, isBot, a.ID, r.IsRevert, r.Weight, diff, r.Timestamp.Format(time.RFC3339Nano)}
			}
			nullItersections(InterestedUsers)
			ame := multiEdge{a.ID, InterestedUsers}

			csvPageChan := csvPageChan
			articleMultiEdgeChan := articleMultiEdgeChan
			url := "https://" + p.Language + ".wikipedia.org/wiki/" + strings.Replace(a.Title, " ", "_", -1)
			for i := 0; i < 2; i++ {
				select {
				case csvPageChan <- &csvPage{a.ID, a.Title, a.Abstract, url, a.TopicID, socialJumps{nil}}:
					csvPageChan = nil
				case articleMultiEdgeChan <- ame:
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
			case csvSocialJumpsChan <- &csvSocialJumps{sj.From, socialJumps{sj.To}}:
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

func chan2csv(c <-chan interface{}, filePath string) (err error) {
	var csvFile *os.File
	if csvFile, err = os.Create(filePath); err != nil {
		return
	}
	defer func() {
		if e := csvFile.Close(); e != nil && err == nil {
			err = e
		}
	}()

	bw := bufio.NewWriter(csvFile)
	defer bw.Flush()

	csvw := csv.NewWriter(bw)
	defer csvw.Flush()

	return gocsv.MarshalChan(c, gocsv.NewSafeCSVWriter(csvw))
}

func groupByVertexA(ctx context.Context, in chan multiEdge, vertexACount, vertexBCount, edgeCount int, fail func(err error) error) <-chan vertexLinks {
	groupedCh := make(chan multiEdge, vertexACount)
	go func() {
		defer close(groupedCh)

		VertexA2VerticesB := make(map[uint32][]*roaring.Bitmap, vertexACount)
		for me := range in {
			newGroup := me.VerticesB
			if len(newGroup) == 0 { //no empty array are inserted
				continue
			}
			group, ok := VertexA2VerticesB[me.VertexA]

			if len(newGroup) > len(group) {
				newGroup, group = group, newGroup
			}
			VertexA2VerticesB[me.VertexA] = group

			if !ok {
				continue
			}
			for i, s := range newGroup {
				group[i].Or(s)
			}
			nullItersections(group)
		}

		for vertexA, verticesB := range VertexA2VerticesB {
			groupedCh <- multiEdge{vertexA, verticesB}
		}
	}()

	return newBi2Similgraph(ctx, groupedCh, vertexACount, vertexBCount, edgeCount, fail)
}

type csvArticleEg struct {
	ID        uint32  `csv:"id"`
	UserID    *uint32 `csv:"userid"`
	IsBot     bool    `csv:"isbot"`
	PageID    uint32  `csv:"pageid"`
	IsRevert  uint32  `csv:"isrevert"`
	Weight    float64 `csv:"weight"`
	Diff      float64 `csv:"diff"`
	Timestamp string  `csv:"timestamp"`
}

type csvPage struct {
	ID          uint32      `csv:"id"`
	Title       string      `csv:"title"`
	Abstract    string      `csv:"abstract"`
	URL         string      `csv:"url"`
	TopicID     uint32      `csv:"topicid"`
	SocialJumps socialJumps `csv:"socialjumps"`
}

type csvSocialJumps struct {
	ID          uint32      `csv:"id"`
	SocialJumps socialJumps `csv:"socialjumps"`
}

type socialJumps struct {
	pages []uint32
}

func (sj socialJumps) String() string {
	pps := make([]string, len(sj.pages))
	for i, p := range sj.pages {
		pps[i] = fmt.Sprint(p)
	}
	return "{" + strings.Join(pps, ", ") + "}"
}

func nullItersections(ss []*roaring.Bitmap) {
	sc := roaring.NewBitmap()
	for i := len(ss) - 1; i >= 0; i-- {
		si := ss[i]
		si.AndNot(sc)
		sc.Or(si)
	}
}

func clone(ss []*roaring.Bitmap) (ssc []*roaring.Bitmap) {
	ssc = make([]*roaring.Bitmap, len(ss))
	for i, s := range ss {
		ssc[i] = s.Clone()
	}
	return
}
