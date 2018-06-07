/*Represents index types that statistics on pages (articles and topics)*/

CREATE TYPE w2o.myindex AS ENUM ('conflict', 'polemic');

CREATE MATERIALIZED VIEW w2o.timebounds AS
SELECT MIN(year) AS minyear, MAX(year) AS maxyear,
MIN(rev_timestamp) AS mintimestamp, MAX(rev_timestamp) AS maxtimestamp
FROM w2o.revisions;

/*Index must defined in a way that a missing entry correctly default to 0.0*/
CREATE MATERIALIZED VIEW w2o.indicesbyyear AS
WITH incompletearticleusersocialindices AS (
    SELECT NULL::w2o.myindex /*ex S. Popularity*/ AS type, page_id, year, COALESCE(user_id,0) AS user_id, COUNT(*) AS count
    FROM w2o.revisions
    GROUP BY page_id, year, user_id
    UNION ALL
    SELECT 'conflict'::w2o.myindex AS type, page_id, year, COALESCE(user_id,0) AS user_id, COUNT(*) AS count
    FROM w2o.revisions
    WHERE rev_isrevert > 0
    GROUP BY page_id, year, user_id
), articleusersocialindices AS (
    SELECT type, page_id, 0 AS year, user_id, SUM(count) AS count
    FROM incompletearticleusersocialindices
    GROUP BY type, page_id, user_id
    UNION ALL
    SELECT *
    FROM incompletearticleusersocialindices
), incompleteuserrevertedpagescount AS (
    SELECT year, user_id, COUNT(*)::float AS count
    FROM articleusersocialindices
    WHERE type IS NOT NULL AND user_id != 0
    GROUP BY year, user_id
), userrevertedpagescount AS (
    SELECT year, 0 AS user_id, AVG(count) AS count /*since anonymous edits don't have an user_id we fill in missing data with a reasonable choice*/
    FROM incompleteuserrevertedpagescount
    GROUP BY year
    UNION ALL
    SELECT *
    FROM incompleteuserrevertedpagescount
), 
articlescreationyear AS (
    SELECT page_id, MIN(year) AS year
    FROM w2o.revisions
    GROUP BY page_id
), articlecountyears AS (
    SELECT _.year, COUNT(*)::float AS totalcount
    FROM w2o.timebounds, articlescreationyear, generate_series(year,maxyear) _(year)
    GROUP BY _.year
    UNION ALL
    SELECT 0 AS year, COUNT(*)::float AS totalcount
    FROM articlescreationyear
), idf AS (
    SELECT year, user_id, log(pc.totalcount/ur.count) AS idf
    FROM articlecountyears pc JOIN userrevertedpagescount ur USING (year)
),
pageusersocialindices AS (
    SELECT type, parent_id AS page_id, year, user_id, SUM(count) AS count
    FROM articleusersocialindices JOIN w2o.pagetree USING (page_id)
    GROUP BY type, parent_id, year, user_id
    UNION ALL
    SELECT *
    FROM articleusersocialindices
), pageeditscount AS (
    SELECT page_id, year, SUM(count)::float AS editscount
    FROM pageusersocialindices
    WHERE type IS NULL
    GROUP BY page_id, year
), indices AS (
    SELECT 'polemic'::w2o.myindex AS type, page_id, year, 1000*SUM(count*idf)/editscount AS weight /*tfidf based on users instead of terms*/
    FROM pageusersocialindices JOIN idf USING (year, user_id)
    JOIN pageeditscount USING (page_id, year)
    WHERE type IS NOT NULL
    GROUP BY page_id, year, editscount
    UNION ALL
    SELECT type, page_id, year, COUNT(*)::float AS weight
    FROM pageusersocialindices
    WHERE type IS NOT NULL AND user_id != 0
    GROUP BY type, page_id, year
),
types AS (
    SELECT DISTINCT type, page_depth
    FROM indices JOIN w2o.pages USING (page_id)
), pagecreationyears AS (
    SELECT page_id, minyear AS year, page_id AS parent_id, page_depth
    FROM w2o.timebounds, w2o.pages
    WHERE page_depth < 2 
    UNION ALL
    SELECT articlescreationyear.*, parent_id, page_depth
    FROM articlescreationyear JOIN w2o.pages USING (page_id)
), typepageyear AS (
    SELECT type, page_id, parent_id, page_depth, _.year
    FROM pagecreationyears JOIN types USING (page_depth),
    w2o.timebounds, generate_series(year,maxyear) _(year)
    UNION ALL
    SELECT type, page_id, parent_id, page_depth, 0 AS year
    FROM pagecreationyears JOIN types USING (page_depth)
)
SELECT type, page_id, parent_id AS topic_id, page_depth, year, COALESCE(weight,0) AS weight
FROM indices RIGHT JOIN typepageyear USING (type, page_id, year);

CREATE INDEX ON w2o.indicesbyyear (page_id);

/*Used by LATERAL JOIN in queries*/
CREATE INDEX ON w2o.indicesbyyear (weight DESC, year, topic_id, type, page_depth);
ANALYZE w2o.indicesbyyear;