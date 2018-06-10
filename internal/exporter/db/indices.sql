/*Represents index types that statistics on pages (articles and topics)*/

CREATE TYPE w2o.myindex AS ENUM ('conflict', 'polemic');

CREATE MATERIALIZED VIEW w2o.timebounds AS
SELECT MIN(year) AS minyear, MAX(year) AS maxyear,
MIN(rev_timestamp) AS mintimestamp, MAX(rev_timestamp) AS maxtimestamp
FROM w2o.revisions;

/*Index must defined in a way that a missing entry correctly default to 0.0*/
CREATE MATERIALIZED VIEW w2o.indicesbyyear AS
WITH incompletearticleeditscount AS (
    SELECT page_id, year, COUNT(*) AS editscount
    FROM w2o.revisions
    GROUP BY page_id, year
), articleeditscount AS (
    SELECT page_id, 0 AS year, SUM(editscount) AS editscount
    FROM incompletearticleeditscount
    GROUP BY page_id
    UNION ALL
    SELECT *
    FROM incompletearticleeditscount
), pageeditscount AS (
    SELECT parent_id AS page_id, year, SUM(editscount) AS editscount
    FROM articleeditscount JOIN w2o.pagetree USING (page_id)
    GROUP BY parent_id, year
    UNION ALL
    SELECT *
    FROM articleeditscount
),
userpagescount AS (
    SELECT year, user_id, COUNT(DISTINCT page_id)::float AS count
    FROM w2o.revisions
    WHERE user_id IS NOT NULL
    GROUP BY year, user_id
    UNION ALL
    SELECT 0 AS year, user_id, COUNT(DISTINCT page_id)::float AS count
    FROM w2o.revisions
    WHERE user_id IS NOT NULL
    GROUP BY user_id
), userrevertedpagescount AS (
    SELECT year, user_id, COUNT(DISTINCT page_id)::float AS count
    FROM w2o.revisions
    WHERE rev_isrevert > 0 AND user_id IS NOT NULL
    GROUP BY year, user_id
    UNION ALL
    SELECT 0 AS year, user_id, COUNT(DISTINCT page_id)::float AS count
    FROM w2o.revisions
    WHERE rev_isrevert > 0 AND user_id IS NOT NULL
    GROUP BY user_id
), incompleteidf AS (
    SELECT year, user_id, log(up.count/ur.count) AS idf
    FROM userpagescount up JOIN userrevertedpagescount ur USING (year, user_id)
), idf AS (
    SELECT year, 0 AS user_id, AVG(idf) AS idf /*we fill in missing data with a reasonable choice for anonymous edits*/
    FROM incompleteidf
    GROUP BY year
    UNION ALL
    SELECT *
    FROM incompleteidf
), 
incompletearticleconflict AS (
    SELECT 'conflict'::w2o.myindex AS type, page_id, year, COALESCE(user_id,0) AS user_id, COUNT(*) AS count
    FROM w2o.revisions
    WHERE rev_isrevert > 0
    GROUP BY page_id, year, user_id
), articleconflict AS (
    SELECT type, page_id, 0 AS year, user_id, SUM(count) AS count
    FROM incompletearticleconflict
    GROUP BY type, page_id, user_id
    UNION ALL
    SELECT *
    FROM incompletearticleconflict
), pageconflict AS (
    SELECT type, parent_id AS page_id, year, user_id, SUM(count) AS count
    FROM articleconflict JOIN w2o.pagetree USING (page_id)
    GROUP BY type, parent_id, year, user_id
    UNION ALL
    SELECT *
    FROM articleconflict
), indices AS (
    SELECT 'polemic'::w2o.myindex AS type, page_id, year, 1000*SUM(count*idf)/editscount AS weight /*tfidf based on users instead of terms*/
    FROM pageconflict JOIN idf USING (year, user_id)
    JOIN pageeditscount USING (page_id, year)
    GROUP BY page_id, year, editscount
    UNION ALL
    SELECT type, page_id, year, COUNT(*)::float AS weight
    FROM pageconflict
    WHERE user_id != 0
    GROUP BY type, page_id, year
),
types AS (
    SELECT DISTINCT type, page_depth
    FROM indices JOIN w2o.pages USING (page_id)
), articlescreationyear AS (
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