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
    WikipediaUrl VARCHAR(512),
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
