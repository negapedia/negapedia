/*Represents index types that statistics on pages (articles and topics)*/

CREATE TYPE w2o.myindex AS ENUM ('conflict', 'polemic');

CREATE MATERIALIZED VIEW w2o.timebounds AS
SELECT MIN(year) AS minyear, MAX(year) AS maxyear,
MIN(rev_timestamp) AS mintimestamp, MAX(rev_timestamp) AS maxtimestamp
FROM w2o.revisions;

/*Index must defined in a way that a missing entry correctly default to 0.0*/
CREATE MATERIALIZED VIEW w2o.indicesbyyear AS
WITH articleusersocialindices AS (
    SELECT DISTINCT NULL::w2o.myindex /*ex S. Popularity*/ AS type, page_id, year, user_id
    FROM w2o.revisions
    WHERE user_id IS NOT NULL
    UNION ALL
    SELECT DISTINCT 'conflict'::w2o.myindex AS type, page_id, year, user_id
    FROM w2o.revisions
    WHERE user_id IS NOT NULL AND rev_isrevert > 0
), pageusersocialindices AS (
    SELECT DISTINCT type, parent_id AS page_id, year, user_id
    FROM articleusersocialindices JOIN w2o.pagetree USING (page_id)
    UNION ALL
    SELECT *
    FROM articleusersocialindices
),
articlescreationyear AS (
    SELECT page_id, MIN(year) AS year
    FROM w2o.revisions
    GROUP BY page_id
), pagecreationyears AS (
    SELECT page_id, minyear AS year, page_id AS parent_id, page_depth
    FROM w2o.timebounds, w2o.pages
    WHERE page_depth < 2 
    UNION ALL
    SELECT articlescreationyear.*, parent_id, page_depth
    FROM articlescreationyear JOIN w2o.pages USING (page_id)
), pagecountyears AS (
    SELECT page_depth, _.year, COUNT(*)::float AS totalpagecount
    FROM w2o.timebounds, pagecreationyears, generate_series(year,maxyear) _(year)
    GROUP BY page_depth, _.year
),
pageusersocialindicescount AS (
    SELECT type, page_id, year, COUNT(*)::float AS weight
    FROM pageusersocialindices
    GROUP BY type, page_id, year
), pairedpageusersocialindicescount AS (
    SELECT page_id, year, p1.weight AS popularity, p2.weight AS conflict
    FROM pageusersocialindicescount p1 JOIN pageusersocialindicescount p2 USING (page_id, year)
    WHERE p1.type IS NULL AND p2.type = 'conflict'::w2o.myindex
), SparseEQPopularityEQConflict AS (
    SELECT page_depth, year, popularity, conflict, COUNT(*) as count
    FROM w2o.pages p JOIN pairedpageusersocialindicescount p1 USING (page_id)
    GROUP BY page_depth, year, popularity, conflict
), Popularity AS (
    SELECT DISTINCT page_depth, year, popularity
    FROM SparseEQPopularityEQConflict
), Conflict AS (
    SELECT DISTINCT page_depth, year, conflict
    FROM SparseEQPopularityEQConflict
), EQPopularityEQConflict AS (
    SELECT page_depth, year, popularity, conflict, COALESCE(count,0) AS count
    FROM w2o.timebounds, generate_series(minyear,maxyear) _(year)
    CROSS JOIN generate_series(0,2) __(page_depth)
    JOIN Popularity USING (page_depth, year)
    JOIN Conflict USING (page_depth, year)
    LEFT JOIN SparseEQPopularityEQConflict USING (page_depth, year, popularity, conflict)
), EQPopularityGEConflict AS (
    SELECT page_depth, year, popularity, conflict, 
    SUM(count) OVER (PARTITION BY popularity, page_depth, year ORDER BY conflict DESC) as count
    FROM EQPopularityEQConflict
), LEPopularityGEConflict AS (
    SELECT page_depth, year, popularity, conflict, 
    SUM(count) OVER (PARTITION BY conflict, page_depth, year ORDER BY popularity) as count
    FROM EQPopularityGEConflict
), articlespolemic AS (
    SELECT 'polemic'::w2o.myindex AS type, page_id, year, 100*(conflict/popularity)*log(totalpagecount/count) AS weight
    FROM pairedpageusersocialindicescount
    JOIN LEPopularityGEConflict USING (year, popularity, conflict)
    JOIN pagecountyears USING (page_depth, year)
    WHERE page_depth = 2
), indices AS (
    SELECT *
    FROM pageusersocialindicescount
    WHERE type IS NOT NULL
    UNION ALL
    SELECT * FROM articlespolemic
    UNION ALL
    SELECT 'polemic'::w2o.myindex AS type, parent_id AS page_id, year, SUM(weight) AS weight
    FROM articlespolemic JOIN w2o.pagetree USING (page_id)
    GROUP BY parent_id, year
),
types AS (
    SELECT DISTINCT type, page_depth
    FROM indices JOIN w2o.pages USING (page_id)
), typepageyear AS (
    SELECT type, page_id, parent_id, page_depth, _.year
    FROM pagecreationyears JOIN types USING (page_depth),
    w2o.timebounds, generate_series(year,maxyear) _(year)
), completeindices AS (
    SELECT type, page_id, parent_id AS topic_id, page_depth, year, COALESCE(weight,0) AS weight
    FROM indices RIGHT JOIN typepageyear USING (type, page_id, year)
)
SELECT type, page_id, topic_id, page_depth, 0 AS year, percentile_disc(0.5) WITHIN GROUP (ORDER BY weight) AS weight
FROM completeindices
GROUP BY type, page_id, topic_id, page_depth
UNION ALL
SELECT * FROM completeindices;

CREATE INDEX ON w2o.indicesbyyear (page_id);

/*Used by LATERAL JOIN in queries*/
CREATE INDEX ON w2o.indicesbyyear (weight DESC, year, topic_id, type, page_depth);
ANALYZE w2o.indicesbyyear;


