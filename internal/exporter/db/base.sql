DROP SCHEMA IF EXISTS w2o CASCADE;
CREATE SCHEMA w2o;

/*Pages represents wikipedia articles and overpedia topics*/

CREATE COLLATION w2o.mycollate (LOCALE = 'en_US.UTF-8');

CREATE TABLE w2o.pages (
    page_id INTEGER NOT NULL,
    page_title VARCHAR(512) NOT NULL COLLATE w2o.mycollate,
    page_abstract TEXT COLLATE w2o.mycollate,
    page_url VARCHAR(512) NOT NULL COLLATE w2o.mycollate,
    parent_id INTEGER NOT NULL,
    page_socialjumps INTEGER[]
);

/*Revisions represents revisions of wikipedia articles*/

CREATE TABLE w2o.revisions (
    rev_id INTEGER NOT NULL,
    user_id INTEGER,
    user_isbot BOOLEAN NOT NULL,
    page_id INTEGER NOT NULL,
    rev_isrevert INTEGER NOT NULL,
    rev_charweight FLOAT NOT NULL,
    rev_chardiff FLOAT NOT NULL,
    rev_timestamp TIMESTAMP NOT NULL,
    year INTEGER
);

CREATE TABLE w2o.socialjumps (
    page_id INTEGER NOT NULL,
    page_socialjumps INTEGER[10]
);

/*Loading data...*/

COPY w2o.pages FROM :'pagesfilepath' WITH CSV HEADER;
COPY w2o.revisions(rev_id,user_id,user_isbot,page_id,rev_isrevert,rev_charweight,rev_chardiff,rev_timestamp) FROM :'revisionsfilepath' WITH CSV HEADER;
UPDATE w2o.revisions SET year = CAST (EXTRACT(YEAR FROM date_trunc('year', rev_timestamp)) AS INTEGER);

ALTER TABLE w2o.pages
    ADD PRIMARY KEY (page_id),
    ADD FOREIGN KEY (parent_id) REFERENCES w2o.pages (page_id),
    ADD COLUMN page_depth INTEGER DEFAULT 2,
    ALTER COLUMN page_depth SET NOT NULL,
    ALTER COLUMN page_title DROP NOT NULL,
    ALTER COLUMN page_url DROP NOT NULL;

COPY w2o.socialjumps FROM :'socialjumpsfilepath' WITH CSV HEADER;
UPDATE w2o.pages SET page_socialjumps = _.page_socialjumps
  FROM (
    SELECT page_id, sj.page_socialjumps
    FROM w2o.socialjumps sj RIGHT JOIN w2o.pages USING (page_id)
  ) _ WHERE _.page_id = pages.page_id;
DROP TABLE w2o.socialjumps;

INSERT INTO w2o.pages VALUES/*fake page used for global statistic*/
    (0, NULL, NULL, NULL, 0, '{}', 0);

UPDATE w2o.pages SET (page_depth, parent_id) = (1, 0) WHERE page_id!=0 AND page_id=parent_id;
CREATE INDEX ON w2o.pages (page_depth, page_title);

CLUSTER w2o.pages USING pages_pkey;
ANALYZE w2o.pages;


CREATE MATERIALIZED VIEW w2o.pagetree AS
SELECT page_id, parent_id
FROM w2o.pages
UNION
SELECT p2.page_id, p1.parent_id
FROM w2o.pages p1 JOIN w2o.pages p2 ON p1.page_id=p2.parent_id;

CREATE INDEX pagetree_cluster_index ON w2o.pagetree (page_id, parent_id);
CLUSTER w2o.pagetree USING pagetree_cluster_index;
ANALYZE w2o.pagetree;

CREATE VIEW w2o.topicpages AS
SELECT p2.*, p2.page_depth<2 AS istopic, COALESCE(p1.page_title, p2.page_title) AS topic_fulltitle,
lower(split_part(COALESCE(p1.page_title, p2.page_title),' ',1)) AS topic_title
FROM w2o.pages p1 JOIN w2o.pages p2 ON p1.page_id=p2.parent_id;

ALTER TABLE w2o.revisions
    ADD PRIMARY KEY (rev_id),
    ADD FOREIGN KEY (page_id) REFERENCES w2o.pages (page_id),
    ALTER COLUMN year SET NOT NULL;

CREATE INDEX ON w2o.revisions (user_id);
CREATE INDEX ON w2o.revisions (rev_isrevert);

ANALYZE w2o.revisions;
