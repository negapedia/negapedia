/*Represents index types that statistics on pages (articles and topics)*/

CREATE TYPE w2o.myindex AS ENUM ('conflict', 'polemic');

/*Index must defined in a way that a missing entry correctly default to 0.0*/
CREATE TABLE w2o.indicesbyyear AS
WITH incompletearticleeditscount AS (
    SELECT page_id, rev_year AS year, COUNT(*) AS editscount, COUNT(*) FILTER (WHERE rev_revert2serialid IS NOT NULL) AS revertcount
    FROM w2o.revisions
    GROUP BY page_id, rev_year
), articleeditscount AS (
    SELECT page_id, 0 AS year, SUM(editscount) AS editscount, SUM(revertcount) AS revertcount
    FROM incompletearticleeditscount
    GROUP BY page_id
    UNION ALL
    SELECT *
    FROM incompletearticleeditscount
), pageeditscount AS (
    SELECT parent_id AS page_id, year, SUM(editscount)::float AS editscount, SUM(revertcount)::float AS revertcount
    FROM articleeditscount JOIN w2o.pagetree USING (page_id)
    GROUP BY parent_id, year
    UNION ALL
    SELECT page_id, year, editscount::float AS editscount, revertcount::float AS revertcount
    FROM articleeditscount
),
incompletearticleconflict AS (
    SELECT DISTINCT 'conflict'::w2o.myindex AS type, page_id, rev_year AS year, user_id
    FROM w2o.revisions
    WHERE rev_revert2serialid IS NOT NULL AND user_id IS NOT NULL
), articleconflict AS (
    SELECT DISTINCT type, page_id, 0 AS year, user_id
    FROM incompletearticleconflict
    UNION ALL
    SELECT *
    FROM incompletearticleconflict
), pageconflict AS (
    SELECT type, parent_id AS page_id, year, COUNT(DISTINCT user_id)::float AS weight
    FROM articleconflict JOIN w2o.pagetree USING (page_id)
    GROUP BY type, parent_id, year
    UNION ALL
    SELECT type, page_id, year, COUNT(user_id)::float AS weight
    FROM articleconflict
    GROUP BY type, page_id, year
), indices AS (
    SELECT *
    FROM pageconflict
    UNION ALL    
    SELECT 'polemic'::w2o.myindex AS type, page_id, year, 1000*revertcount*LOG(1+weight)/editscount AS weight
    FROM pageeditscount JOIN pageconflict USING (page_id, year)
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

DROP TABLE w2o.revisions;

CREATE INDEX ON w2o.indicesbyyear (page_id);

/*Used by LATERAL JOIN in queries*/
CREATE INDEX ON w2o.indicesbyyear (weight DESC, year, topic_id, type, page_depth);
ANALYZE w2o.indicesbyyear;