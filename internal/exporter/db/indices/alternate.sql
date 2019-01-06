/*Define indicesbyyear table that for each page contains yearly conflict and polemic statistic*/
/*Indices must defined in a way that missing entries correctly default to 0.0*/
CREATE TABLE w2o.indicesbyyear AS
WITH incompletearticleconflict AS (
    SELECT DISTINCT page_id, rev_year AS year, user_id
    FROM w2o.revisions
    WHERE (rev_isrevert > 0 OR rev_isreverted) AND user_id IS NOT NULL
), articleconflict AS (
    SELECT DISTINCT page_id, 0 AS year, user_id
    FROM incompletearticleconflict
    UNION ALL
    SELECT *
    FROM incompletearticleconflict
), pageconflict AS (
    SELECT parent_id AS page_id, year, COUNT(DISTINCT user_id)::FLOAT AS weight
    FROM articleconflict JOIN w2o.pagetree USING (page_id)
    GROUP BY parent_id, year
    UNION ALL
    SELECT page_id, year, COUNT(user_id)::FLOAT AS weight
    FROM articleconflict
    GROUP BY page_id, year
),
incompletearticleeditscount AS (
    SELECT page_id, rev_year AS year, COUNT(*) FILTER (WHERE rev_isrevert = 0) AS positivecount,
    COUNT(*) FILTER (WHERE rev_isrevert > 0) AS revertcount
    FROM w2o.revisions
    WHERE NOT rev_isreverted
    GROUP BY page_id, rev_year
), articleeditscount AS (
    SELECT page_id, 0 AS year, SUM(positivecount) AS positivecount, SUM(revertcount) AS revertcount
    FROM incompletearticleeditscount
    GROUP BY page_id
    UNION ALL
    SELECT *
    FROM incompletearticleeditscount
), pageeditscount AS (
    SELECT parent_id AS page_id, (parent_id>0)::INTEGER AS page_depth, year,
    SUM(positivecount)::FLOAT AS positivecount, SUM(revertcount)::FLOAT AS revertcount
    FROM articleeditscount JOIN w2o.pagetree USING (page_id)
    GROUP BY parent_id, year
    UNION ALL
    SELECT page_id, 2 AS page_depth, year,
    positivecount::FLOAT AS positivecount, revertcount::FLOAT AS revertcount
    FROM articleeditscount
),
indices AS (
    SELECT 'conflict'::w2o.myindex AS type, *
    FROM pageconflict
    UNION ALL
    SELECT 'polemic'::w2o.myindex AS type, page_id, year,
    1000*revertcount/(positivecount + MAX(revertcount) OVER(PARTITION BY page_depth, year)) AS weight
    FROM pageeditscount
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