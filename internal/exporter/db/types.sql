/*Free space since revisions table will not be anymore useful and indexes will take a lot of space*/
DROP TABLE w2o.revisions;

/*Define indexes over indicesbyyear*/
CREATE INDEX ON w2o.indicesbyyear (page_id);
/*Used by LATERAL JOIN in queries*/
CREATE INDEX ON w2o.indicesbyyear (weight DESC, year, topic_id, type, page_depth);
ANALYZE w2o.indicesbyyear;


/*The following types are used by the pages and toptenbyyear queries*/

CREATE TYPE w2o.yearmeasurement AS (
	Value            FLOAT,
	Percentile       FLOAT,
	DensePercentile  FLOAT,
	Rank             INT,
	TopicPercentile  FLOAT,
	TopicDensePercentile  FLOAT,
	TopicRank        INT,
	Year             INT
);

CREATE TYPE w2o.indextype2measurements AS (
    Indextype       w2o.myindex,
    Measurements    w2o.yearmeasurement[]
);

CREATE TYPE w2o.mypage AS (
    Title    VARCHAR(512),
    Abstract  TEXT,
    Topic    VARCHAR(512),
    IsTopic BOOLEAN
);

CREATE TYPE w2o.myextendedpage AS (
    Title    VARCHAR(512),
    Abstract  TEXT,
    Topic    VARCHAR(512),
    IsTopic BOOLEAN,
    FullTopic    VARCHAR(512),
    CreationYear  INTEGER,
    PageDepth     INTEGER
);


CREATE TYPE w2o.pageinfo  AS (
    Page        w2o.myextendedpage,
    Stats       w2o.indextype2measurements[],
    Links       w2o.mypage[]
);

CREATE TYPE w2o.indexranking AS (
    index    w2o.myindex,
    ranking  w2o.mypage[]
);

CREATE TYPE w2o.annualindexesranking AS (
    year    INTEGER,
    indexesranking w2o.indexranking[]
);
