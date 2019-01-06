DROP SCHEMA IF EXISTS w2o CASCADE;
CREATE SCHEMA w2o;

/*Myindex represents index types of statistics*/
CREATE TYPE w2o.myindex AS ENUM ('conflict', 'polemic');

CREATE COLLATION w2o.mycollate (LOCALE = 'en_US.UTF-8');

/*Pages represents wikipedia articles and overpedia topics*/
CREATE TABLE w2o.pages (
    page_id INTEGER NOT NULL,
    page_title VARCHAR(512) COLLATE w2o.mycollate,
    page_abstract TEXT COLLATE w2o.mycollate,
    parent_id INTEGER NOT NULL,
    page_socialjumps INTEGER[] NOT NULL DEFAULT '{}',
    page_depth INTEGER NOT NULL DEFAULT 2,
    page_creationyear INTEGER
);

/*Revisions represents wikipedia article edits */
CREATE TABLE w2o.revisions (
    page_id INTEGER NOT NULL,
    rev_serialid INTEGER NOT NULL,
    user_id INTEGER,
    user_isbot BOOLEAN NOT NULL,
    rev_charweight FLOAT NOT NULL,
    rev_chardiff FLOAT NOT NULL,
    rev_isrevert INTEGER NOT NULL,
    rev_isreverted BOOLEAN NOT NULL,
    rev_timestamp TIMESTAMP NOT NULL,
    rev_year INTEGER
);

/*Socialjumps is a temporary table used for loading socialjumps, later data is merged into pages table*/
CREATE TABLE w2o.socialjumps (
    page_id INTEGER NOT NULL,
    page_socialjumps INTEGER[10]
);


/*Load data and define table indexes*/

/*Dummy page used for global statistics*/
INSERT INTO w2o.pages(page_id, parent_id, page_depth) VALUES (0, 0, 0);

COPY w2o.pages(page_id,page_title,page_abstract,parent_id) FROM :'pagesfilepath' WITH CSV HEADER;
COPY w2o.revisions(page_id,rev_serialid,user_id,user_isbot,rev_charweight,rev_chardiff, rev_isrevert, rev_isreverted, rev_timestamp) FROM :'revisionsfilepath' WITH CSV HEADER;

ALTER TABLE w2o.pages
    ADD PRIMARY KEY (page_id),
    ADD FOREIGN KEY (parent_id) REFERENCES w2o.pages (page_id);
UPDATE w2o.pages SET page_depth = 1 WHERE parent_id=0 AND page_id!=0;
CLUSTER w2o.pages USING pages_pkey;
ANALYZE w2o.pages;
CREATE INDEX ON w2o.pages (page_depth, page_title);

UPDATE w2o.revisions SET rev_year = CAST (EXTRACT(YEAR FROM date_trunc('year', rev_timestamp)) AS INTEGER);
ALTER TABLE w2o.revisions
    ADD PRIMARY KEY (page_id,rev_serialid),
    ADD FOREIGN KEY (page_id) REFERENCES w2o.pages (page_id),
    ALTER COLUMN rev_year SET NOT NULL;
CLUSTER w2o.revisions USING revisions_pkey;
ANALYZE w2o.revisions;
CREATE INDEX ON w2o.revisions (user_id);

CREATE TABLE w2o.timebounds AS
SELECT MIN(rev_year) AS minyear, MAX(rev_year) AS maxyear,
MIN(rev_timestamp) AS mintimestamp, MAX(rev_timestamp) AS maxtimestamp
FROM w2o.revisions;

COPY w2o.socialjumps FROM :'socialjumpsfilepath' WITH CSV HEADER;
UPDATE w2o.pages SET (page_socialjumps,page_creationyear) = (_.page_socialjumps, _.page_creationyear)
  FROM (
    WITH pagecreation AS (
    SELECT page_id, minyear AS page_creationyear
    FROM w2o.timebounds, w2o.pages
    WHERE page_depth < 2 
    UNION ALL
    SELECT page_id, MIN(rev_year) AS page_creationyear
    FROM w2o.revisions
    GROUP BY page_id)
    SELECT page_id, COALESCE(sj.page_socialjumps,'{}') AS page_socialjumps, page_creationyear
    FROM pagecreation LEFT JOIN w2o.socialjumps sj USING (page_id)
  ) _ WHERE _.page_id = pages.page_id;
DROP TABLE w2o.socialjumps;
ALTER TABLE w2o.pages
    ALTER COLUMN page_creationyear SET NOT NULL;


/*Pagetree contains the complete graph of parent relations*/
CREATE MATERIALIZED VIEW w2o.pagetree AS
SELECT page_id, parent_id
FROM w2o.pages
UNION
SELECT p2.page_id, p1.parent_id
FROM w2o.pages p1 JOIN w2o.pages p2 ON p1.page_id=p2.parent_id;

CREATE INDEX pagetree_cluster_index ON w2o.pagetree (page_id, parent_id);
CLUSTER w2o.pagetree USING pagetree_cluster_index;
ANALYZE w2o.pagetree;


/*topicpages is a utility view for simplifing queries by joining article and its topic information*/
CREATE VIEW w2o.topicpages AS
SELECT p2.*, p2.page_depth<2 AS istopic, COALESCE(p1.page_title, p2.page_title) AS topic_fulltitle,
lower(split_part(COALESCE(p1.page_title, p2.page_title),' ',1)) AS topic_title
FROM w2o.pages p1 JOIN w2o.pages p2 ON p1.page_id=p2.parent_id;
