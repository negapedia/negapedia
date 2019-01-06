/*Define the query used for exporting articles yealy top tens*/
WITH years AS (
    SELECT year
    FROM w2o.timebounds, generate_series(minyear,maxyear) _(year)
    UNION ALL
    SELECT 0 AS year
), topics AS (
    SELECT page_id AS topic_id
    FROM w2o.pages
    WHERE page_depth = 1
), types AS (
    SELECT DISTINCT type
    FROM w2o.indicesbyyear
    WHERE page_depth = 2
), top10 AS (
    SELECT _.year, _.type, array_agg(CAST((tp.page_title, tp.page_abstract, tp.topic_title, tp.istopic) AS w2o.mypage) ORDER BY weight DESC) AS pages 
    FROM years, topics, types,
    LATERAL (
        SELECT year, type, page_id, weight
        FROM w2o.indicesbyyear
        WHERE year = years.year AND topic_id = topics.topic_id AND type = types.type AND page_depth = 2
        ORDER BY weight DESC
        LIMIT 10
    ) _ JOIN w2o.topicpages tp USING (page_id)
    GROUP BY _.year, _.type
), yearjson AS (
    SELECT year, row_to_json(CAST((year, array_agg(CAST((type, pages) AS w2o.indexranking) ORDER BY type)) AS w2o.annualindexesranking)) AS json
    FROM top10
    GROUP BY year
) SELECT json
FROM yearjson
ORDER BY year;
