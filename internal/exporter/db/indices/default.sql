/*Define indicesbyyear table that for each page contains yearly conflict and polemic statistic*/
/*Indices must defined in a way that missing entries correctly default to 0.0*/
CREATE TABLE w2o.indicesbyyear AS
WITH articleusersocialindices AS (
    SELECT DISTINCT NULL::w2o.myindex /*ex S. Popularity*/ AS type, page_id, rev_year AS year, user_id
    FROM w2o.revisions
    WHERE user_id IS NOT NULL
    UNION ALL
    SELECT DISTINCT 'conflict'::w2o.myindex AS type, page_id, rev_year AS year, user_id
    FROM w2o.revisions
    WHERE user_id IS NOT NULL AND rev_isrevert > 0
), incompletepageusersocialindices AS (
    SELECT DISTINCT type, parent_id AS page_id, year, user_id
    FROM articleusersocialindices JOIN w2o.pagetree USING (page_id)
    UNION ALL
    SELECT *
    FROM articleusersocialindices
), pageusersocialindices AS (
    SELECT DISTINCT type, page_id, 0 AS year, user_id
    FROM incompletepageusersocialindices
    UNION ALL
    SELECT *
    FROM incompletepageusersocialindices
),
articlecountyears AS (
    SELECT _.year, COUNT(*)::FLOAT AS totalpagecount
    FROM w2o.timebounds, w2o.pages, generate_series(page_creationyear,maxyear) _(year)
    WHERE page_depth = 2
    GROUP BY _.year
    UNION ALL
    SELECT 0 AS year, COUNT(*)::FLOAT AS totalpagecount
    FROM w2o.pages
    WHERE page_depth = 2
),
pageusersocialindicescount AS (
    SELECT type, page_id, year, COUNT(*)::FLOAT AS weight
    FROM pageusersocialindices
    GROUP BY type, page_id, year
), pairedarticlesocialindicescount AS (
    SELECT page_id, year, p1.weight AS popularity, p2.weight AS conflict
    FROM w2o.pages JOIN pageusersocialindicescount p1 USING (page_id)
    JOIN pageusersocialindicescount p2 USING (page_id, year)
    WHERE p1.type IS NULL AND p2.type = 'conflict'::w2o.myindex AND page_depth = 2
), SparseEQPopularityEQConflict AS (
    SELECT year, popularity, conflict, COUNT(*) as count
    FROM w2o.pages p JOIN pairedarticlesocialindicescount p1 USING (page_id)
    GROUP BY year, popularity, conflict
), Popularity AS (
    SELECT DISTINCT year, popularity
    FROM SparseEQPopularityEQConflict
), Conflict AS (
    SELECT DISTINCT year, conflict
    FROM SparseEQPopularityEQConflict
), Years AS (
    SELECT year
    FROM w2o.timebounds, generate_series(minyear,maxyear) _(year)
    UNION ALL
    SELECT 0 AS year
), EQPopularityEQConflict AS (
    SELECT year, popularity, conflict, COALESCE(count,0) AS count
    FROM Years JOIN Popularity USING (year)
    JOIN Conflict USING (year)
    LEFT JOIN SparseEQPopularityEQConflict USING (year, popularity, conflict)
), EQPopularityGEConflict AS (
    SELECT year, popularity, conflict, 
    SUM(count) OVER (PARTITION BY popularity, year ORDER BY conflict DESC) as count
    FROM EQPopularityEQConflict
), LEPopularityGEConflict AS (
    SELECT year, popularity, conflict, 
    SUM(count) OVER (PARTITION BY conflict, year ORDER BY popularity) as count
    FROM EQPopularityGEConflict
), untimedarticlespolemic AS (
    SELECT page_id, year, (conflict/popularity)*log(totalpagecount/count) AS weight
    FROM pairedarticlesocialindicescount
    JOIN LEPopularityGEConflict USING (year, popularity, conflict)
    JOIN articlecountyears USING (year)
), 
minmaxarticletimestamp AS (
    SELECT page_id, MIN(rev_year) AS minyear, MAX(rev_year) AS maxyear,
    MIN(rev_timestamp) AS mintimestamp, MAX(rev_timestamp) AS maxtimestamp
    FROM w2o.revisions
    GROUP BY page_id
), timeweights AS (
    SELECT page_id, year,
    EXTRACT(epoch FROM (LEAST(maxtimestamp,make_date(year+1,1,1))-GREATEST(mintimestamp,make_date(year,1,1))))/86400.0 AS weight
    FROM minmaxarticletimestamp, generate_series(minyear,maxyear) _(year)
    UNION ALL
    SELECT page_id, 0 AS year, EXTRACT(epoch FROM (maxtimestamp-mintimestamp))/86400.0 AS weight
    FROM minmaxarticletimestamp
), articlespolemic AS (
    SELECT 'polemic'::w2o.myindex AS type, page_id, year, ap.weight*tw.weight AS weight
    FROM untimedarticlespolemic ap JOIN timeweights tw USING (page_id, year)
),
indices AS (
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
    FROM w2o.pages JOIN types USING (page_depth),
    w2o.timebounds, generate_series(page_creationyear,maxyear) _(year)
    UNION ALL
    SELECT type, page_id, parent_id, page_depth, 0 AS year
    FROM w2o.pages JOIN types USING (page_depth)
)
SELECT type, page_id, parent_id AS topic_id, page_depth, year, COALESCE(weight,0) AS weight
FROM indices RIGHT JOIN typepageyear USING (type, page_id, year);