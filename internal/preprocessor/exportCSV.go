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
			case csvPageChan <- &csvPage{ID: t.ID, Title: t.Title, Abstract: t.Abstract}:
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

			SHA12ID, positiveChange := conflictualData(a.Revisions)

			InterestedUsers := []*roaring.Bitmap{roaring.NewBitmap(), roaring.NewBitmap(), roaring.NewBitmap()}
			oldWeight := float64(0)
			for ID, r := range a.Revisions {
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

				//Revert data
				var revert2ID *uint32
				if _Revert2ID, isRevert := SHA12ID[r.SHA1]; isRevert {
					revert2ID = &_Revert2ID //setted to the first serial ID having the same SHA1 sum
				}

				//Export to csv
				if !isBot || !p.FilterBots {
					csvArticleRevisionChan <- &csvArticleEg{a.ID, ID, userID, isBot, r.Weight, diff, revert2ID, positiveChange.Contains(ID), r.Timestamp.Format(time.RFC3339Nano)}
				}

				//Convert data for socialjumps
				switch {
				case isBot:
					//do nothing
				case revert2ID != nil && diff <= 120:
					InterestedUsers[0].Add(r.UserID)
				case revert2ID != nil || diff <= 120:
					InterestedUsers[1].Add(r.UserID)
				default:
					InterestedUsers[2].Add(r.UserID)
				}
			}
			nullItersections(InterestedUsers)
			ame := multiEdge{a.ID, InterestedUsers}

			csvPageChan := csvPageChan
			articleMultiEdgeChan := articleMultiEdgeChan
			for i := 0; i < 2; i++ {
				select {
				case csvPageChan <- &csvPage{a.ID, a.Title, a.Abstract, a.TopicID}:
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
	PageID       uint32  `csv:"pageid"`
	ID           uint32  `csv:"ID"`
	UserID       *uint32 `csv:"userid"`
	IsBot        bool    `csv:"isbot"`
	Weight       float64 `csv:"weight"`
	Diff         float64 `csv:"diff"`
	Revert2ID    *uint32 `csv:"revert2id"`
	Constructive bool    `csv:"constructive"`
	Timestamp    string  `csv:"timestamp"`
}

type csvPage struct {
	ID       uint32 `csv:"id"`
	Title    string `csv:"title"`
	Abstract string `csv:"abstract"`
	TopicID  uint32 `csv:"topicid"`
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

func conflictualData(revisions []wikibrief.Revision) (SHA12ID map[string]uint32, positiveChange *roaring.Bitmap) {
	//SHA12ID maps sha1 to the last revision serial number in which it appears
	SHA12ID = make(map[string]uint32, len(revisions))
	for ID, r := range revisions {
		_, isRevert := SHA12ID[r.SHA1]
		switch {
		case isRevert:
			//do nothing
		case len(r.SHA1) == 31:
			SHA12ID[r.SHA1] = uint32(ID)
		}
	}

	//add to positiveChange edits that weren't reverted
	positiveChange = roaring.NewBitmap()
	for ID := uint32(len(revisions) - 1); ID > 0; ID-- {
		newID, isRevert := SHA12ID[revisions[ID].SHA1]
		switch {
		case isRevert && ID == newID:
			positiveChange.Add(ID)
		case isRevert: // && ID != newID:
			ID = newID
		default:
			positiveChange.Add(ID)
		}
	}

	return
}
