/*
docker run -v /home/ebonetti/Downloads/testdata/csv:/csv -v /home/ebonetti/go/src/github.com/ebonetti/overpedia/internal/exporter/db:/db --rm -it --name mydb  postgres
docker exec -it mydb /bin/bash
chmod -R 777 /csv; cd /db; psql -U postgres -v datafilepath='/csv/' -c '\i test.sql';
*/

\pset pager off

\set pagesfilepath :datafilepath pages.csv
\set revisionsfilepath :datafilepath revisions.csv
\set socialjumpsfilepath :datafilepath socialjumps.csv

\i base.sql;
\i indices/default.sql;
\i types.sql;
\i query-toptenbyyear.sql;
\i query-pages.sql;
